Allows to run simple ClickHouse stress test in Docker from debian packages.
Actually it runs multiple copies of clickhouse-test (functional tests).
This allows to find problems like failed assertions and memory safety issues.

Usage:
```
$ ls $HOME/someclickhouse
clickhouse-client_18.14.9_all.deb clickhouse-common-static_18.14.9_amd64.deb clickhouse-server_18.14.9_all.deb
$ docker run --volume=$HOME/someclickhouse:/package_folder --volume=$HOME/test_output:/test_output clickhouse/stress-test
Selecting previously unselected package clickhouse-common-static.
(Reading database ... 14442 files and directories currently installed.)
...
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
$ ls $HOME/test_result
clickhouse-server.err.log clickhouse-server.log.0.zst stderr.log stress_test_run_0.txt  stress_test_run_11.txt stress_test_run_13.txt
stress_test_run_15.txt stress_test_run_2.txt stress_test_run_4.txt stress_test_run_6.txt stress_test_run_8.txt clickhouse-server.log
perf_stress_run.txt stdout.log stress_test_run_10.txt stress_test_run_12.txt
stress_test_run_14.txt stress_test_run_1.txt
stress_test_run_3.txt stress_test_run_5.txt stress_test_run_7.txt stress_test_run_9.txt
```
