# [draft] Performance comparison test

This is an experimental mode that compares performance of old and new server
side by side. Both servers are run, and the query is executed on one then another,
measuring the times. This setup should remove much of the variability present in
the current performance tests, which only run the new version and compare with
the old results recorded some time in the past.

To interpret the observed results, we build randomization distribution for the
observed difference of median times between old and new server, under the null
hypothesis that the performance distribution is the same (for the details of the
method, see [1]). We consider the observed difference in performance significant,
if it is above 5% and above the 95th percentile of the randomization distribution.
We also consider the test to be unstable, if the observed difference is less than
5%, but the 95th percentile is above 5% -- this means that we are likely to observe
performance differences above 5% more often than in 5% runs, so the test is likely
to have false positives.

### How to read the report

Should add inline comments there, because who reads the docs anyway. They must
be collapsible and I am afraid of Javascript, so I'm going to do it later.

### How to run
Run the entire docker container, specifying PR number (0 for master)
and SHA of the commit to test. The reference revision is determined as a nearest
ancestor testing release tag. It is possible to specify the reference revision and
pull requests (0 for master) manually.

```
docker run --network=host --volume=$(pwd)/workspace:/workspace --volume=$(pwd)/output:/output
    [-e REF_PR={} -e REF_SHA={}]
    -e PR_TO_TEST={} -e SHA_TO_TEST={}
    yandex/clickhouse-performance-comparison
```

Then see the `report.html` in the `output` directory.

There are some environment variables that influence what the test does:
 * `-e CHCP_RUNS` -- the number of runs;
 * `-e CHPC_TEST_GREP` -- the names of the tests (xml files) to run, interpreted
 as a grep pattern.
 * `-e CHPC_LOCAL_SCRIPT` -- use the comparison scripts from the docker container and not from the tested commit.

#### Re-genarate report with your tweaks
From the workspace directory (extracted test output archive):
```
stage=report compare.sh
```
More stages are available, e.g. restart servers or run the tests. See the code.

#### Run a single test on the already configured servers
```
docker/test/performance-comparison/perf.py --host=localhost --port=9000 --runs=1 tests/performance/logical_functions_small.xml
```

#### Run all tests on some custom configuration
Start two servers manually on ports `9001` (old) and `9002` (new). Change to a
new directory to be used as workspace for tests, and try something like this:
```
$ PATH=$PATH:~/ch4/build-gcc9-rel/programs \
    CHPC_TEST_PATH=~/ch3/ch/tests/performance \
    CHPC_TEST_GREP=visit_param \
    stage=run_tests \
    ~/ch3/ch/docker/test/performance-comparison/compare.sh
```
* `PATH` must contain `clickhouse-local` and `clickhouse-client`.
* `CHPC_TEST_PATH` -- path to performance test cases, e.g. `tests/performance`.
* `CHPC_TEST_GREP` -- a filter for which tests to run, as a grep pattern.
* `stage` -- from which execution stage to start. To run the tests, use
  `run_tests` stage.

The tests will run, and the `report.html` will be generated in the current
directory.

More complex setup is possible, but inconvenient and requires some scripting.
See `manual-run.sh` for inspiration.


#### Statistical considerations
Generating randomization distribution for medians is tricky. Suppose we have N
runs for each version, and then use the combined 2N run results to make a
virtual experiment. In this experiment, we only have N possible values for
median of each version. This becomes very clear if you sort those 2N runs and
imagine where a window of N runs can be -- the N/2 smallest and N/2 largest
values can never be medians. From these N possible values of
medians, you can obtain (N/2)^2 possible values of absolute median difference.
These numbers are +-1, I'm making an off-by-one error somewhere. So, if your
number of runs is small, e.g. 7, you'll only get 16 possible differences, so
even if you make 100k virtual experiments, the randomization distribution will
have only 16 steps, so you'll get weird effects. So you also have to have
enough runs. You can observe it on real data if you add more output to the
query that calculates randomization distribution, e.g., add a number of unique
median values. Looking even more closely, you can see that the exact
values of medians don't matter, and the randomization distribution for
difference of medians devolves into some kind of ranked test. We could probably
skip all these virtual experiments and calculate the resulting distribution
analytically, but I don't know enough math to do it. It would be something
close to Wilcoxon test distribution.

### References
1\. Box, Hunter, Hunter "Statictics for exprerimenters", p. 78: "A Randomized Design Used in the Comparison of Standard and Modified Fertilizer Mixtures for Tomato Plants."
