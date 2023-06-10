from operator import eq
import os
import random
import time
import sys
from clickhouse_driver import Client
import numpy as np
import subprocess
import multiprocessing
from multiprocessing import Manager

warmup_runs = 10
calculated_runs = 10
seconds = 30
max_instances_number = 8
retest_number = 3
retest_tolerance = 10


def checkInt(str):
    try:
        int(str)
        return True
    except ValueError:
        return False


def setup_client(index):
    if index < 4:
        port_idx = index
    else:
        port_idx = index + 4
    client = Client(
        host="localhost",
        database="default",
        user="default",
        password="",
        port="900%d" % port_idx,
    )
    union_mode_query = "SET union_default_mode='DISTINCT'"
    client.execute(union_mode_query)
    return client


def warm_client(clientN, clientL, query, loop):
    for c_idx in range(clientN):
        for _ in range(loop):
            clientL[c_idx].execute(query)


def read_queries(queries_list):
    queries = list()
    queries_id = list()
    with open(queries_list, "r") as f:
        for line in f:
            line = line.rstrip()
            line = line.split("$")
            queries_id.append(line[0])
            queries.append(line[1])
    return queries_id, queries


def run_task(client, cname, query, loop, query_latency):
    start_time = time.time()
    for i in range(loop):
        client.execute(query)
        query_latency.append(client.last_query.elapsed)

    end_time = time.time()
    p95 = np.percentile(query_latency, 95)
    print(
        "CLIENT: {0} end. -> P95: %f, qps: %f".format(cname)
        % (p95, loop / (end_time - start_time))
    )


def run_multi_clients(clientN, clientList, query, loop):
    client_pids = {}
    start_time = time.time()
    manager = multiprocessing.Manager()
    query_latency_list0 = manager.list()
    query_latency_list1 = manager.list()
    query_latency_list2 = manager.list()
    query_latency_list3 = manager.list()
    query_latency_list4 = manager.list()
    query_latency_list5 = manager.list()
    query_latency_list6 = manager.list()
    query_latency_list7 = manager.list()

    for c_idx in range(clientN):
        client_name = "Role_%d" % c_idx
        if c_idx == 0:
            client_pids[c_idx] = multiprocessing.Process(
                target=run_task,
                args=(clientList[c_idx], client_name, query, loop, query_latency_list0),
            )
        elif c_idx == 1:
            client_pids[c_idx] = multiprocessing.Process(
                target=run_task,
                args=(clientList[c_idx], client_name, query, loop, query_latency_list1),
            )
        elif c_idx == 2:
            client_pids[c_idx] = multiprocessing.Process(
                target=run_task,
                args=(clientList[c_idx], client_name, query, loop, query_latency_list2),
            )
        elif c_idx == 3:
            client_pids[c_idx] = multiprocessing.Process(
                target=run_task,
                args=(clientList[c_idx], client_name, query, loop, query_latency_list3),
            )
        elif c_idx == 4:
            client_pids[c_idx] = multiprocessing.Process(
                target=run_task,
                args=(clientList[c_idx], client_name, query, loop, query_latency_list4),
            )
        elif c_idx == 5:
            client_pids[c_idx] = multiprocessing.Process(
                target=run_task,
                args=(clientList[c_idx], client_name, query, loop, query_latency_list5),
            )
        elif c_idx == 6:
            client_pids[c_idx] = multiprocessing.Process(
                target=run_task,
                args=(clientList[c_idx], client_name, query, loop, query_latency_list6),
            )
        elif c_idx == 7:
            client_pids[c_idx] = multiprocessing.Process(
                target=run_task,
                args=(clientList[c_idx], client_name, query, loop, query_latency_list7),
            )
        else:
            print("ERROR: CLIENT number dismatch!!")
            exit()
        print("CLIENT: %s start" % client_name)
        client_pids[c_idx].start()

    for c_idx in range(clientN):
        client_pids[c_idx].join()
    end_time = time.time()
    totalT = end_time - start_time

    query_latencyTotal = list()
    for item in query_latency_list0:
        query_latencyTotal.append(item)
    for item in query_latency_list1:
        query_latencyTotal.append(item)
    for item in query_latency_list2:
        query_latencyTotal.append(item)
    for item in query_latency_list3:
        query_latencyTotal.append(item)
    for item in query_latency_list4:
        query_latencyTotal.append(item)
    for item in query_latency_list5:
        query_latencyTotal.append(item)
    for item in query_latency_list6:
        query_latencyTotal.append(item)
    for item in query_latency_list7:
        query_latencyTotal.append(item)

    totalP95 = np.percentile(query_latencyTotal, 95) * 1000
    return totalT, totalP95


def run_task_caculated(client, cname, query, loop):
    query_latency = list()
    start_time = time.time()
    for i in range(loop):
        client.execute(query)
        query_latency.append(client.last_query.elapsed)
    end_time = time.time()
    p95 = np.percentile(query_latency, 95)


def run_multi_clients_caculated(clientN, clientList, query, loop):
    client_pids = {}
    start_time = time.time()
    for c_idx in range(clientN):
        client_name = "Role_%d" % c_idx
        client_pids[c_idx] = multiprocessing.Process(
            target=run_task_caculated,
            args=(clientList[c_idx], client_name, query, loop),
        )
        client_pids[c_idx].start()
    for c_idx in range(clientN):
        client_pids[c_idx].join()
    end_time = time.time()
    totalT = end_time - start_time
    return totalT


if __name__ == "__main__":
    client_number = 1
    queries = list()
    queries_id = list()

    if len(sys.argv) != 3:
        print(
            "usage: python3 client_stressing_test.py [queries_file_path] [client_number]"
        )
        sys.exit()
    else:
        queries_list = sys.argv[1]
        client_number = int(sys.argv[2])
        print(
            "queries_file_path: %s, client_number: %d" % (queries_list, client_number)
        )
        if not os.path.isfile(queries_list) or not os.access(queries_list, os.R_OK):
            print("please check the right path for queries file")
            sys.exit()
        if (
            not checkInt(sys.argv[2])
            or int(sys.argv[2]) > max_instances_number
            or int(sys.argv[2]) < 1
        ):
            print("client_number should be in [1~%d]" % max_instances_number)
            sys.exit()

    client_list = {}
    queries_id, queries = read_queries(queries_list)

    for c_idx in range(client_number):
        client_list[c_idx] = setup_client(c_idx)
    # clear cache
    os.system("sync; echo 3 > /proc/sys/vm/drop_caches")

    print("###Polit Run Begin")
    for i in queries:
        warm_client(client_number, client_list, i, 1)
    print("###Polit Run End -> Start stressing....")

    query_index = 0
    for q in queries:
        print(
            "\n###START -> Index: %d, ID: %s, Query: %s"
            % (query_index, queries_id[query_index], q)
        )
        warm_client(client_number, client_list, q, warmup_runs)
        print("###Warm Done!")
        for j in range(0, retest_number):
            totalT = run_multi_clients_caculated(
                client_number, client_list, q, calculated_runs
            )
            curr_loop = int(seconds * calculated_runs / totalT) + 1
            print(
                "###Calculation Done! -> loopN: %d, expected seconds:%d"
                % (curr_loop, seconds)
            )

            print("###Stress Running! -> %d iterations......" % curr_loop)

            totalT, totalP95 = run_multi_clients(
                client_number, client_list, q, curr_loop
            )

            if totalT > (seconds - retest_tolerance) and totalT < (
                seconds + retest_tolerance
            ):
                break
            else:
                print(
                    "###totalT:%d is far way from expected seconds:%d. Run again ->j:%d!"
                    % (totalT, seconds, j)
                )

        print(
            "###Completed! -> ID: %s, clientN: %d, totalT: %.2f s, latencyAVG: %.2f ms, P95: %.2f ms, QPS_Final: %.2f"
            % (
                queries_id[query_index],
                client_number,
                totalT,
                totalT * 1000 / (curr_loop * client_number),
                totalP95,
                ((curr_loop * client_number) / totalT),
            )
        )
        query_index += 1
    print("###Finished!")
