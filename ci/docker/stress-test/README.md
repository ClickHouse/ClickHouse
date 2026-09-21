Allows to run a simple ClickHouse stress test in Docker from a `clickhouse`
binary artifact.
Actually it runs multiple copies of clickhouse-test (functional tests).
This allows to find problems like failed assertions and memory safety issues.

Usage:
```
$ ls $HOME/someclickhouse
clickhouse
$ mkdir -p $HOME/test_output $HOME/server_log $HOME/cores
$ docker run --cap-add=SYS_PTRACE --privileged --ulimit nofile=1048576:1048576 \
    -e S3_URL='https://s3.amazonaws.com/clickhouse-datasets' --tmpfs /tmp/clickhouse:mode=1777 \
    --volume=$HOME/someclickhouse:/package_folder --volume=$HOME/test_output:/test_output \
    --volume=$PWD:/repo --volume=$HOME/server_log:/var/log/clickhouse-server \
    --volume=$HOME/cores:/cores clickhouse/stress-test /repo/tests/docker_scripts/stress_runner.sh
Start clickhouse-server service: Path to data directory in /etc/clickhouse-server/config.xml: /var/lib/clickhouse/
DONE
2018-10-22 13:40:35,744 Will wait functests to finish
2018-10-22 13:40:40,747 Finished 0 from 16 processes
2018-10-22 13:40:45,751 Finished 0 from 16 processes
...
2018-10-22 13:49:11,165 Finished 15 from 16 processes
2018-10-22 13:49:16,171 Checking ClickHouse still alive
Still alive
2018-10-22 13:49:16,195 Stress is ok
2018-10-22 13:49:16,195 Copying server log files
$ ls $HOME/test_output
clickhouse-server.err.log clickhouse-server.log.0.zst stderr.log stress_test_run_0.txt  stress_test_run_11.txt stress_test_run_13.txt
stress_test_run_15.txt stress_test_run_2.txt stress_test_run_4.txt stress_test_run_6.txt stress_test_run_8.txt clickhouse-server.log
perf_stress_run.txt stdout.log stress_test_run_10.txt stress_test_run_12.txt
stress_test_run_14.txt stress_test_run_1.txt
stress_test_run_3.txt stress_test_run_5.txt stress_test_run_7.txt stress_test_run_9.txt
```
