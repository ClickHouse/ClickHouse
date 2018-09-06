import time
import threading
import random
from collections import Counter

import pytest

from helpers.cluster import ClickHouseCluster


cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance('node1', with_zookeeper=True)
node2 = cluster.add_instance('node2', with_zookeeper=True)
nodes = [node1, node2]

@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        for node in nodes:
            node.query("DROP TABLE IF EXISTS test_mutations")

        for node in nodes:
            node.query("CREATE TABLE test_mutations(d Date, x UInt32, i UInt32) ENGINE ReplicatedMergeTree('/clickhouse/tables/test/test_mutations', '{instance}') ORDER BY x PARTITION BY toYYYYMM(d)")

        yield cluster

    finally:
        cluster.shutdown()


class Runner:
    def __init__(self):
        self.mtx = threading.Lock()
        self.total_inserted_xs = 0
        self.total_inserted_rows = 0

        self.total_mutations = 0
        self.total_deleted_xs = 0
        self.total_deleted_rows = 0

        self.current_xs = Counter()

        self.currently_inserting_xs = Counter()
        self.currently_deleting_xs = set()

        self.stop_ev = threading.Event()

    def do_insert(self, thread_num):
        self.stop_ev.wait(random.random())

        # Each thread inserts a small random number of rows with random year, month 01 and day determined
        # by the thread number. The idea is to avoid spurious duplicates and to insert into a
        # nontrivial number of partitions.
        month = '01'
        day = str(thread_num + 1).zfill(2)
        i = 1
        while not self.stop_ev.is_set():
            xs = [random.randint(1, 10) for _ in range(random.randint(1, 10))]
            with self.mtx:
                xs = [x for x in xs if x not in self.currently_deleting_xs]
                if len(xs) == 0:
                    continue
                for x in xs:
                    self.currently_inserting_xs[x] += 1

            year = 2000 + random.randint(0, 10)
            date_str = '{year}-{month}-{day}'.format(year=year, month=month, day=day)
            payload = ''
            for x in xs:
                payload += '{date_str}	{x}	{i}\n'.format(date_str=date_str, x=x, i=i)
                i += 1

            try:
                print 'thread {}: insert for {}: {}'.format(thread_num, date_str, ','.join(str(x) for x in xs))
                random.choice(nodes).query("INSERT INTO test_mutations FORMAT TSV", payload)

                with self.mtx:
                    for x in xs:
                        self.current_xs[x] += 1
                    self.total_inserted_xs += sum(xs)
                    self.total_inserted_rows += len(xs)

            except Exception, e:
                print 'Exception while inserting,', e
            finally:
                with self.mtx:
                    for x in xs:
                        self.currently_inserting_xs[x] -= 1

            self.stop_ev.wait(0.2 + random.random() / 5)

    def do_delete(self, thread_num):
        self.stop_ev.wait(1.0 + random.random())

        while not self.stop_ev.is_set():
            chosen = False
            with self.mtx:
                if self.current_xs:
                    x = random.choice(list(self.current_xs.elements()))

                    if self.currently_inserting_xs[x] == 0 and x not in self.currently_deleting_xs:
                        chosen = True
                        self.currently_deleting_xs.add(x)
                        to_delete_count = self.current_xs[x]

            if not chosen:
                self.stop_ev.wait(0.1 * random.random())
                continue

            try:
                print 'thread {}: delete {} * {}'.format(thread_num, to_delete_count, x)
                random.choice(nodes).query("ALTER TABLE test_mutations DELETE WHERE x = {}".format(x))

                with self.mtx:
                    self.total_mutations += 1
                    self.current_xs[x] -= to_delete_count
                    self.total_deleted_xs += to_delete_count * x
                    self.total_deleted_rows += to_delete_count

            except Exception, e:
                print 'Exception while deleting,', e
            finally:
                with self.mtx:
                    self.currently_deleting_xs.remove(x)

            self.stop_ev.wait(1.0 + random.random() * 2)


def test_mutations(started_cluster):
    DURATION_SECONDS = 50

    runner = Runner()

    threads = []
    for thread_num in range(5):
        threads.append(threading.Thread(target=runner.do_insert, args=(thread_num, )))

    for thread_num in (11, 12, 13):
        threads.append(threading.Thread(target=runner.do_delete, args=(thread_num,)))

    for t in threads:
        t.start()

    time.sleep(DURATION_SECONDS)
    runner.stop_ev.set()

    for t in threads:
        t.join()

    # Sanity check: at least something was inserted and something was deleted
    assert runner.total_inserted_rows > 0
    assert runner.total_mutations > 0

    all_done = False
    for i in range(100): # wait for replication 50 seconds max
        time.sleep(0.5)

        def get_done_mutations(node):
            return int(node.query("SELECT sum(is_done) FROM system.mutations WHERE table = 'test_mutations'").rstrip())

        if all([get_done_mutations(n) == runner.total_mutations for n in nodes]):
            all_done = True
            break

    print node1.query("SELECT mutation_id, command, parts_to_do, is_done FROM system.mutations WHERE table = 'test_mutations' FORMAT TSVWithNames")
    assert all_done

    expected_sum = runner.total_inserted_xs - runner.total_deleted_xs
    actual_sums = []
    for i, node in enumerate(nodes):
        actual_sums.append(int(node.query("SELECT sum(x) FROM test_mutations").rstrip()))
        assert actual_sums[i] == expected_sum
