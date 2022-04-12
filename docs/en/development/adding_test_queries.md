# How to add test queries to ClickHouse CI

ClickHouse has hundreds (or even thousands) of features. Every commit gets checked by a complex set of tests containing many thousands of test cases.

The core functionality is very well tested, but some corner-cases and different combinations of features can be uncovered with ClickHouse CI.

Most of the bugs/regressions we see happen in that 'grey area' where test coverage is poor.

And we are very interested in covering most of the possible scenarios and feature combinations used in real life by tests.

## Why adding tests

Why/when you should add a test case into ClickHouse code:
1) you use some complicated scenarios / feature combinations / you have some corner case which is probably not widely used
2) you see that certain behavior gets changed between version w/o notifications in the changelog
3) you just want to help to improve ClickHouse quality and ensure the features you use will not be broken in the future releases
4) once the test is added/accepted, you can be sure the corner case you check will never be accidentally broken.
5) you will be a part of great open-source community
6) your name will be visible in the `system.contributors` table!
7) you will make a world bit better :)

### Steps to do

#### Prerequisite

I assume you run some Linux machine (you can use docker / virtual machines on other OS) and any modern browser / internet connection, and you have some basic Linux & SQL skills.

Any highly specialized knowledge is not needed (so you don't need to know C++ or know something about how ClickHouse CI works).


#### Preparation

1) [create GitHub account](https://github.com/join) (if you haven't one yet)
2) [setup git](https://docs.github.com/en/free-pro-team@latest/github/getting-started-with-github/set-up-git)
```bash
# for Ubuntu
sudo apt-get update
sudo apt-get install git

git config --global user.name "John Doe" # fill with your name
git config --global user.email "email@example.com" # fill with your email

```
3) [fork ClickHouse project](https://docs.github.com/en/free-pro-team@latest/github/getting-started-with-github/fork-a-repo) - just open [https://github.com/ClickHouse/ClickHouse](https://github.com/ClickHouse/ClickHouse) and press fork button in the top right corner:
![fork repo](https://github-images.s3.amazonaws.com/help/bootcamp/Bootcamp-Fork.png)

4) clone your fork to some folder on your PC, for example, `~/workspace/ClickHouse`
```
mkdir ~/workspace && cd ~/workspace
git clone https://github.com/< your GitHub username>/ClickHouse
cd ClickHouse
git remote add upstream https://github.com/ClickHouse/ClickHouse
```

#### New branch for the test

1) create a new branch from the latest clickhouse master
```
cd ~/workspace/ClickHouse
git fetch upstream
git checkout -b name_for_a_branch_with_my_test upstream/master
```

#### Install & run clickhouse

1) install `clickhouse-server` (follow [official docs](https://clickhouse.com/docs/en/getting-started/install/))
2) install test configurations (it will use Zookeeper mock implementation and adjust some settings)
```
cd ~/workspace/ClickHouse/tests/config
sudo ./install.sh
```
3) run clickhouse-server
```
sudo systemctl restart clickhouse-server
```

#### Creating the test file


1) find the number for your test - find the file with the biggest number in `tests/queries/0_stateless/`

```sh
$ cd ~/workspace/ClickHouse
$ ls tests/queries/0_stateless/[0-9]*.reference | tail -n 1
tests/queries/0_stateless/01520_client_print_query_id.reference
```
Currently, the last number for the test is `01520`, so my test will have the number `01521`

2) create an SQL file with the next number and name of the feature you test

```sh
touch tests/queries/0_stateless/01521_dummy_test.sql
```

3) edit SQL file with your favorite editor (see hint of creating tests below)
```sh
vim tests/queries/0_stateless/01521_dummy_test.sql
```


4) run the test, and put the result of that into the reference file:
```
clickhouse-client -nmT < tests/queries/0_stateless/01521_dummy_test.sql | tee tests/queries/0_stateless/01521_dummy_test.reference
```

5) ensure everything is correct, if the test output is incorrect (due to some bug for example), adjust the reference file using text editor.

#### How to create a good test

- A test should be
	- minimal - create only tables related to tested functionality, remove unrelated columns and parts of query
	- fast - should not take longer than a few seconds (better subseconds)
	- correct - fails then feature is not working
        - deterministic
	- isolated / stateless
		- don't rely on some environment things
		- don't rely on timing when possible
- try to cover corner cases (zeros / Nulls / empty sets / throwing exceptions)
- to test that query return errors, you can put special comment after the query: `-- { serverError 60 }` or `-- { clientError 20 }`
- don't switch databases (unless necessary)
- you can create several table replicas on the same node if needed
- you can use one of the test cluster definitions when needed (see system.clusters)
- use `number` / `numbers_mt` / `zeros` / `zeros_mt` and similar for queries / to initialize data when applicable
- clean up the created objects after test and before the test (DROP IF EXISTS) - in case of some dirty state
- prefer sync mode of operations (mutations, merges, etc.)
- use other SQL files in the `0_stateless` folder as an example
- ensure the feature / feature combination you want to test is not yet covered with existing tests

#### Test naming rules

It's important to name tests correctly, so one could turn some tests subset off in clickhouse-test invocation.

| Tester flag| What should be in test name | When flag should be added |
|---|---|---|---|
| `--[no-]zookeeper`| "zookeeper" or "replica" | Test uses tables from ReplicatedMergeTree family |
| `--[no-]shard` | "shard" or "distributed" or "global"| Test using connections to 127.0.0.2 or similar |
| `--[no-]long` | "long" or "deadlock" or "race" | Test runs longer than 60 seconds |

#### Commit / push / create PR.

1) commit & push your changes
```sh
cd ~/workspace/ClickHouse
git add tests/queries/0_stateless/01521_dummy_test.sql
git add tests/queries/0_stateless/01521_dummy_test.reference
git commit # use some nice commit message when possible
git push origin HEAD
```
2) use a link which was shown during the push, to create a PR into the main repo
3) adjust the PR title and contents, in `Changelog category (leave one)` keep
`Build/Testing/Packaging Improvement`, fill the rest of the fields if you want.
