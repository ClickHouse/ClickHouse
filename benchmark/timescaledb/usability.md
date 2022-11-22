This is a "usability testing" of TimescaleDB. I did not use TimescaleDB before. I will try to install it, load the data and conduct benchmarks. And record every obstacle that I will face.
Usability testing need to be conducted by the most clueless person in the room. Doing this "usability testing" requires a bit of patience and courage (to publish all the struggles as is).

Note: insted of using clear VM, I have to run benchmark on exactly the same baremetal server where all other benchmarks were run.


## Installation

Install as following:
https://docs.timescale.com/timescaledb/latest/how-to-guides/install-timescaledb/self-hosted/ubuntu/installation-apt-ubuntu/#installation-apt-ubuntu

I've noticed that TimescaleDB documentation website does not have favicon in contrast to the main page.
In other means, it is quite neat.

```
sudo apt install postgresql-common
sudo sh /usr/share/postgresql-common/pgdg/apt.postgresql.org.sh
sudo sh -c "echo 'deb [signed-by=/usr/share/keyrings/timescale.keyring] https://packagecloud.io/timescale/timescaledb/ubuntu/ $(lsb_release -c -s) main' > /etc/apt/sources.list.d/timescaledb.list"
wget --quiet -O - https://packagecloud.io/timescale/timescaledb/gpgkey | sudo gpg --dearmor -o /usr/share/keyrings/timescale.keyring
sudo apt-get update
sudo apt install timescaledb-2-postgresql-13
```

It recommends to tune it:

```
sudo apt install timescaledb-tune

sudo timescaledb-tune --quiet --yes
Using postgresql.conf at this path:
/etc/postgresql/13/main/postgresql.conf

Writing backup to:
/tmp/timescaledb_tune.backup202110292328

Recommendations based on 125.88 GB of available memory and 32 CPUs for PostgreSQL 13
shared_preload_libraries = 'timescaledb'        # (change requires restart)
shared_buffers = 32226MB
effective_cache_size = 96678MB
maintenance_work_mem = 2047MB
work_mem = 10312kB
timescaledb.max_background_workers = 8
max_worker_processes = 43
max_parallel_workers_per_gather = 16
max_parallel_workers = 32
wal_buffers = 16MB
min_wal_size = 512MB
default_statistics_target = 500
random_page_cost = 1.1
checkpoint_completion_target = 0.9
max_locks_per_transaction = 512
autovacuum_max_workers = 10
autovacuum_naptime = 10
effective_io_concurrency = 256
timescaledb.last_tuned = '2021-10-29T23:28:49+03:00'
timescaledb.last_tuned_version = '0.12.0'
Saving changes to: /etc/postgresql/13/main/postgresql.conf
```

```
sudo service postgresql restart
```

Post-install setup:
https://docs.timescale.com/timescaledb/latest/how-to-guides/install-timescaledb/post-install-setup/

```
$ psql -U postgres -h localhost
Password for user postgres:
psql: error: connection to server at "localhost" (::1), port 5432 failed: fe_sendauth: no password supplied
```

How to set up password?

```
milovidov@mtlog-perftest03j:~$ psql -U postgres -h localhost
Password for user postgres:
psql: error: connection to server at "localhost" (::1), port 5432 failed: fe_sendauth: no password supplied
milovidov@mtlog-perftest03j:~$ psql
psql: error: connection to server on socket "/var/run/postgresql/.s.PGSQL.5432" failed: FATAL:  role "milovidov" does not exist
milovidov@mtlog-perftest03j:~$ sudo psql
psql: error: connection to server on socket "/var/run/postgresql/.s.PGSQL.5432" failed: FATAL:  role "root" does not exist
milovidov@mtlog-perftest03j:~$ psql -U postgres
psql: error: connection to server on socket "/var/run/postgresql/.s.PGSQL.5432" failed: FATAL:  Peer authentication failed for user "postgres"
milovidov@mtlog-perftest03j:~$ psql -U postgres -h localost
psql: error: could not translate host name "localost" to address: Name or service not known
milovidov@mtlog-perftest03j:~$ sudo psql -U postgres -h localost
psql: error: could not translate host name "localost" to address: Name or service not known
milovidov@mtlog-perftest03j:~$ sudo psql -U postgres -h localhost
Password for user postgres:
psql: error: connection to server at "localhost" (::1), port 5432 failed: fe_sendauth: no password supplied
milovidov@mtlog-perftest03j:~$ sudo -u postgres psql -h localhost
Password for user postgres:
psql: error: connection to server at "localhost" (::1), port 5432 failed: fe_sendauth: no password supplied
```

I found an answer here: https://stackoverflow.com/questions/12720967/how-to-change-postgresql-user-password

```
$ sudo -u postgres psql
psql (13.4 (Ubuntu 13.4-4.pgdg18.04+1), server 9.5.25)
Type "help" for help.

postgres=# \password postgres
Enter new password:
Enter it again:
postgres=#

CREATE database tutorial;

postgres=# CREATE EXTENSION IF NOT EXISTS timescaledb;
ERROR:  could not open extension control file "/usr/share/postgresql/9.5/extension/timescaledb.control": No such file or directory
```

Looks like I have old PostgreSQL.

```
$ ls -l /usr/share/postgresql/
10/  11/  13/  9.5/
```

But there is also newer PostgreSQL.

```
$ psql --version
psql (PostgreSQL) 13.4 (Ubuntu 13.4-4.pgdg18.04+1)

psql is new, so what is wrong?
```

Looks like I have all versions running simultaneously?

https://askubuntu.com/questions/17823/how-to-list-all-installed-packages

```
$ ps auxw | grep postgres
postgres  718818  0.0  0.5 33991600 730184 ?     Ss   23:29   0:00 /usr/lib/postgresql/13/bin/postgres -D /var/lib/postgresql/13/main -c config_file=/etc/postgresql/13/main/postgresql.conf
postgres  718825  0.0  0.0 320356 27660 ?        S    23:29   0:00 /usr/lib/postgresql/10/bin/postgres -D /var/lib/postgresql/10/main -c config_file=/etc/postgresql/10/main/postgresql.conf
postgres  718826  0.0  0.0 320712 27900 ?        S    23:29   0:00 /usr/lib/postgresql/11/bin/postgres -D /var/lib/postgresql/11/main -c config_file=/etc/postgresql/11/main/postgresql.conf
postgres  718829  0.0  0.0 320468  7092 ?        Ss   23:29   0:00 postgres: 10/main: checkpointer process
postgres  718830  0.0  0.0 320356  4300 ?        Ss   23:29   0:00 postgres: 10/main: writer process
postgres  718831  0.0  0.0 320356  9204 ?        Ss   23:29   0:00 postgres: 10/main: wal writer process
postgres  718832  0.0  0.0 320776  6964 ?        Ss   23:29   0:00 postgres: 10/main: autovacuum launcher process
postgres  718833  0.0  0.0 175404  3596 ?        Ss   23:29   0:00 postgres: 10/main: stats collector process
postgres  718834  0.0  0.0 320640  5052 ?        Ss   23:29   0:00 postgres: 10/main: bgworker: logical replication launcher
postgres  718835  0.0  0.0 320820  5592 ?        Ss   23:29   0:00 postgres: 11/main: checkpointer
postgres  718836  0.0  0.0 320712  4164 ?        Ss   23:29   0:00 postgres: 11/main: background writer
postgres  718837  0.0  0.0 320712  9040 ?        Ss   23:29   0:00 postgres: 11/main: walwriter
postgres  718838  0.0  0.0 321116  6824 ?        Ss   23:29   0:00 postgres: 11/main: autovacuum launcher
postgres  718839  0.0  0.0 175752  3652 ?        Ss   23:29   0:00 postgres: 11/main: stats collector
postgres  718840  0.0  0.0 321120  6640 ?        Ss   23:29   0:00 postgres: 11/main: logical replication launcher
postgres  718842  0.0  0.1 33991700 263860 ?     Ss   23:29   0:00 postgres: 13/main: checkpointer
postgres  718843  0.0  0.2 33991600 264096 ?     Ss   23:29   0:00 postgres: 13/main: background writer
postgres  718844  0.0  0.0 33991600 22044 ?      Ss   23:29   0:00 postgres: 13/main: walwriter
postgres  718845  0.0  0.0 33992284 7040 ?       Ss   23:29   0:00 postgres: 13/main: autovacuum launcher
postgres  718846  0.0  0.0 177920  4320 ?        Ss   23:29   0:00 postgres: 13/main: stats collector
postgres  718847  0.0  0.0 33992136 7972 ?       Ss   23:29   0:00 postgres: 13/main: TimescaleDB Background Worker Launcher
postgres  718848  0.0  0.0 33992164 7248 ?       Ss   23:29   0:00 postgres: 13/main: logical replication launcher
postgres  718857  0.0  0.0 304492 26284 ?        S    23:29   0:00 /usr/lib/postgresql/9.5/bin/postgres -D /var/lib/postgresql/9.5/main -c config_file=/etc/postgresql/9.5/main/postgresql.conf
postgres  718859  0.0  0.0 304592  6480 ?        Ss   23:29   0:00 postgres: checkpointer process
postgres  718860  0.0  0.0 304492  5656 ?        Ss   23:29   0:00 postgres: writer process
postgres  718861  0.0  0.0 304492  4144 ?        Ss   23:29   0:00 postgres: wal writer process
postgres  718862  0.0  0.0 304928  6896 ?        Ss   23:29   0:00 postgres: autovacuum launcher process
postgres  718863  0.0  0.0 159744  4156 ?        Ss   23:29   0:00 postgres: stats collector process
milovid+  724277  0.0  0.0  14364  1024 pts/17   S+   23:41   0:00 grep --color=auto postgres

$ apt list --installed | grep postgres

WARNING: apt does not have a stable CLI interface. Use with caution in scripts.

postgresql-10/now 10.16-1.pgdg18.04+1 amd64 [installed,upgradable to: 10.18-1.pgdg18.04+1]
postgresql-11/now 11.11-1.pgdg18.04+1 amd64 [installed,upgradable to: 11.13-1.pgdg18.04+1]
postgresql-11-postgis-3/now 3.1.1+dfsg-1.pgdg18.04+1 amd64 [installed,upgradable to: 3.1.4+dfsg-1.pgdg18.04+1]
postgresql-11-postgis-3-scripts/now 3.1.1+dfsg-1.pgdg18.04+1 all [installed,upgradable to: 3.1.4+dfsg-1.pgdg18.04+1]
postgresql-13/bionic-pgdg,now 13.4-4.pgdg18.04+1 amd64 [installed,automatic]
postgresql-9.5/bionic-pgdg,now 9.5.25-1.pgdg18.04+1 amd64 [installed]
postgresql-9.5-postgis-2.2-scripts/now 2.2.2+dfsg-4.pgdg14.04+1.yandex all [installed,local]
postgresql-client-10/now 10.16-1.pgdg18.04+1 amd64 [installed,upgradable to: 10.18-1.pgdg18.04+1]
postgresql-client-11/now 11.11-1.pgdg18.04+1 amd64 [installed,upgradable to: 11.13-1.pgdg18.04+1]
postgresql-client-13/bionic-pgdg,now 13.4-4.pgdg18.04+1 amd64 [installed,automatic]
postgresql-client-9.5/bionic-pgdg,now 9.5.25-1.pgdg18.04+1 amd64 [installed]
postgresql-client-common/bionic-pgdg,now 231.pgdg18.04+1 all [installed]
postgresql-common/bionic-pgdg,now 231.pgdg18.04+1 all [installed]
timescaledb-2-loader-postgresql-13/bionic,now 2.5.0~ubuntu18.04 amd64 [installed,automatic]
timescaledb-2-postgresql-13/bionic,now 2.5.0~ubuntu18.04 amd64 [installed]
```

Let's remove all older packages.

```
sudo apt remove postgresql-10 postgresql-11 postgresql-9.5 postgresql-client-10 postgresql-client-11 postgresql-client-9.5
```

Just in case:

```
sudo service postgresql restart
```

Now it stopped to work:

```
$ sudo -u postgres psql
psql: error: connection to server on socket "/var/run/postgresql/.s.PGSQL.5432" failed: No such file or directory
        Is the server running locally and accepting connections on that socket?

$ sudo -u postgres psql -h localhost
psql: error: connection to server at "localhost" (::1), port 5432 failed: Connection refused
        Is the server running on that host and accepting TCP/IP connections?
connection to server at "localhost" (127.0.0.1), port 5432 failed: Connection refused
        Is the server running on that host and accepting TCP/IP connections?
```

But it's running:

```
$ ps auxw | grep postgres
postgres  726158  0.5  0.5 33991600 730084 ?     Ss   23:45   0:00 /usr/lib/postgresql/13/bin/postgres -D /var/lib/postgresql/13/main -c config_file=/etc/postgresql/13/main/postgresql.conf
postgres  726160  0.0  0.0 33991600 4256 ?       Ss   23:45   0:00 postgres: 13/main: checkpointer
postgres  726161  0.1  0.1 33991600 150048 ?     Ss   23:45   0:00 postgres: 13/main: background writer
postgres  726162  0.0  0.0 33991600 22044 ?      Ss   23:45   0:00 postgres: 13/main: walwriter
postgres  726163  0.0  0.0 33992284 6976 ?       Ss   23:45   0:00 postgres: 13/main: autovacuum launcher
postgres  726164  0.0  0.0 177920  4384 ?        Ss   23:45   0:00 postgres: 13/main: stats collector
postgres  726165  0.0  0.0 33992136 7840 ?       Ss   23:45   0:00 postgres: 13/main: TimescaleDB Background Worker Launcher
postgres  726166  0.0  0.0 33992164 7244 ?       Ss   23:45   0:00 postgres: 13/main: logical replication launcher
milovid+  726578  0.0  0.0  14364  1100 pts/17   S+   23:46   0:00 grep --color=auto postgres
```

But it does not listen 5432:

```
$ netstat -n | grep 5432
```

Let's look at the config:

```
sudo mcedit /etc/postgresql/13/main/postgresql.conf
```

```
# - Connection Settings -

#listen_addresses = 'localhost'
```

Looks like I need to uncomment it.

```
sudo service postgresql restart
```

But it did not help:

```
$ sudo -u postgres psql -h localhost
psql: error: connection to server at "localhost" (::1), port 5432 failed: Connection refused
        Is the server running on that host and accepting TCP/IP connections?
connection to server at "localhost" (127.0.0.1), port 5432 failed: Connection refused
        Is the server running on that host and accepting TCP/IP connections?
```

Let's consult https://stackoverflow.com/questions/31091748/postgres-server-not-listening

It is mentioning some pg_hba.conf. BTW what is HBA*? Let's find this file...

```
sudo mcedit /etc/postgresql/13/main/pg_hba.conf
```

\* host based authentication rules - it is explained inside this file.

Nothing wrong in this file...

```
$ sudo service postgresql status
● postgresql.service - PostgreSQL RDBMS
   Loaded: loaded (/lib/systemd/system/postgresql.service; enabled; vendor preset: enabled)
   Active: active (exited) since Fri 2021-10-29 23:50:14 MSK; 6min ago
  Process: 728545 ExecStart=/bin/true (code=exited, status=0/SUCCESS)
 Main PID: 728545 (code=exited, status=0/SUCCESS)

Oct 29 23:50:14 mtlog-perftest03j systemd[1]: postgresql.service: Changed dead -> start
Oct 29 23:50:14 mtlog-perftest03j systemd[1]: Starting PostgreSQL RDBMS...
Oct 29 23:50:14 mtlog-perftest03j systemd[728545]: postgresql.service: Executing: /bin/true
Oct 29 23:50:14 mtlog-perftest03j systemd[1]: postgresql.service: Child 728545 belongs to postgresql.service.
Oct 29 23:50:14 mtlog-perftest03j systemd[1]: postgresql.service: Main process exited, code=exited, status=0/SUCCESS
Oct 29 23:50:14 mtlog-perftest03j systemd[1]: postgresql.service: Changed start -> exited
Oct 29 23:50:14 mtlog-perftest03j systemd[1]: postgresql.service: Job postgresql.service/start finished, result=done
Oct 29 23:50:14 mtlog-perftest03j systemd[1]: Started PostgreSQL RDBMS.
Oct 29 23:50:14 mtlog-perftest03j systemd[1]: postgresql.service: Failed to send unit change signal for postgresql.service: Connection reset by peer
```

It's quite cryptic. What does it mean "Failed to send unit change signal"? Is it good or bad?
What is the "unit"? Maybe it is "SystemD Unit" - the phrase that I've heard many times but don't really understand.

Almost gave up... Wow, I found the culprit! In `/etc/postgresql/13/main/postgresql.conf`:

```
port = 5435
```

Most likely this has happened, because multiple versions of PostgreSQL were installed.

Let's change to 5432.

```
sudo mcedit /etc/postgresql/13/main/postgresql.conf
sudo service postgresql restart
```

But now it does not accept password:

```
milovidov@mtlog-perftest03j:~$ sudo -u postgres psql -h 127.0.0.1
Password for user postgres:
psql: error: connection to server at "127.0.0.1", port 5432 failed: fe_sendauth: no password supplied
milovidov@mtlog-perftest03j:~$ sudo -u postgres psql -h 127.0.0.1 --password ''
Password:
psql: error: connection to server at "127.0.0.1", port 5432 failed: fe_sendauth: no password supplied
milovidov@mtlog-perftest03j:~$ sudo -u postgres psql -h 127.0.0.1
Password for user postgres:
psql: error: connection to server at "127.0.0.1", port 5432 failed: fe_sendauth: no password supplied
```

Works this way:

```
$ sudo -u postgres psql
psql (13.4 (Ubuntu 13.4-4.pgdg18.04+1))
Type "help" for help.

postgres=# \password
Enter new password:
Enter it again:
```

It works with fine ASCII arc:

```
postgres=# CREATE database tutorial;
CREATE DATABASE
postgres=# \c tutorial
You are now connected to database "tutorial" as user "postgres".
tutorial=# CREATE EXTENSION IF NOT EXISTS timescaledb;
WARNING:
WELCOME TO
 _____ _                               _     ____________
|_   _(_)                             | |    |  _  \ ___ \
  | |  _ _ __ ___   ___  ___  ___ __ _| | ___| | | | |_/ /
  | | | |  _ ` _ \ / _ \/ __|/ __/ _` | |/ _ \ | | | ___ \
  | | | | | | | | |  __/\__ \ (_| (_| | |  __/ |/ /| |_/ /
  |_| |_|_| |_| |_|\___||___/\___\__,_|_|\___|___/ \____/
               Running version 2.5.0
For more information on TimescaleDB, please visit the following links:

 1. Getting started: https://docs.timescale.com/timescaledb/latest/getting-started
 2. API reference documentation: https://docs.timescale.com/api/latest
 3. How TimescaleDB is designed: https://docs.timescale.com/timescaledb/latest/overview/core-concepts

Note: TimescaleDB collects anonymous reports to better understand and assist our users.
For more information and how to disable, please see our docs https://docs.timescale.com/timescaledb/latest/how-to-guides/configuration/telemetry.

CREATE EXTENSION
```


## Creating Table

Continuing to https://docs.timescale.com/timescaledb/latest/how-to-guides/hypertables/create/

Create table:

```
CREATE TABLE  hits_100m_obfuscated (
WatchID BIGINT,
JavaEnable SMALLINT,
Title TEXT,
GoodEvent SMALLINT,
EventTime TIMESTAMP,
EventDate Date,
CounterID INTEGER,
ClientIP INTEGER,
RegionID INTEGER,
UserID BIGINT,
CounterClass SMALLINT,
OS SMALLINT,
UserAgent SMALLINT,
URL TEXT,
Referer TEXT,
Refresh SMALLINT,
RefererCategoryID SMALLINT,
RefererRegionID INTEGER,
URLCategoryID SMALLINT,
URLRegionID INTEGER,
ResolutionWidth SMALLINT,
ResolutionHeight SMALLINT,
ResolutionDepth SMALLINT,
FlashMajor SMALLINT,
FlashMinor SMALLINT,
FlashMinor2 TEXT,
NetMajor SMALLINT,
NetMinor SMALLINT,
UserAgentMajor SMALLINT,
UserAgentMinor CHAR(2),
CookieEnable SMALLINT,
JavascriptEnable SMALLINT,
IsMobile SMALLINT,
MobilePhone SMALLINT,
MobilePhoneModel TEXT,
Params TEXT,
IPNetworkID INTEGER,
TraficSourceID SMALLINT,
SearchEngineID SMALLINT,
SearchPhrase TEXT,
AdvEngineID SMALLINT,
IsArtifical SMALLINT,
WindowClientWidth SMALLINT,
WindowClientHeight SMALLINT,
ClientTimeZone SMALLINT,
ClientEventTime TIMESTAMP,
SilverlightVersion1 SMALLINT,
SilverlightVersion2 SMALLINT,
SilverlightVersion3 INTEGER,
SilverlightVersion4 SMALLINT,
PageCharset TEXT,
CodeVersion INTEGER,
IsLink SMALLINT,
IsDownload SMALLINT,
IsNotBounce SMALLINT,
FUniqID BIGINT,
OriginalURL TEXT,
HID INTEGER,
IsOldCounter SMALLINT,
IsEvent SMALLINT,
IsParameter SMALLINT,
DontCountHits SMALLINT,
WithHash SMALLINT,
HitColor CHAR,
LocalEventTime TIMESTAMP,
Age SMALLINT,
Sex SMALLINT,
Income SMALLINT,
Interests SMALLINT,
Robotness SMALLINT,
RemoteIP INTEGER,
WindowName INTEGER,
OpenerName INTEGER,
HistoryLength SMALLINT,
BrowserLanguage TEXT,
BrowserCountry TEXT,
SocialNetwork TEXT,
SocialAction TEXT,
HTTPError SMALLINT,
SendTiming INTEGER,
DNSTiming INTEGER,
ConnectTiming INTEGER,
ResponseStartTiming INTEGER,
ResponseEndTiming INTEGER,
FetchTiming INTEGER,
SocialSourceNetworkID SMALLINT,
SocialSourcePage TEXT,
ParamPrice BIGINT,
ParamOrderID TEXT,
ParamCurrency TEXT,
ParamCurrencyID SMALLINT,
OpenstatServiceName TEXT,
OpenstatCampaignID TEXT,
OpenstatAdID TEXT,
OpenstatSourceID TEXT,
UTMSource TEXT,
UTMMedium TEXT,
UTMCampaign TEXT,
UTMContent TEXT,
UTMTerm TEXT,
FromTag TEXT,
HasGCLID SMALLINT,
RefererHash BIGINT,
URLHash BIGINT,
CLID INTEGER
);
```

I remember PostgreSQL does not support unsigned integers. It also does not support TINYINT.
And it does not support zero bytes in TEXT fields. We will deal with it...

```
tutorial=# SELECT create_hypertable('hits_100m_obfuscated', 'EventTime');
ERROR:  column "EventTime" does not exist
```

WTF?

Maybe it because column names are lowercased?

```
tutorial=# SELECT create_hypertable('hits_100m_obfuscated', 'eventtime');
NOTICE:  adding not-null constraint to column "eventtime"
DETAIL:  Time dimensions cannot have NULL values.
         create_hypertable
-----------------------------------
 (1,public,hits_100m_obfuscated,t)
(1 row)
```

Looks like I forgot to specify NOT NULL for every column.
Let's repeat...

```
tutorial=# DROP TABLE hits_100m_obfuscated
tutorial-# ;
DROP TABLE
tutorial=# CREATE TABLE  hits_100m_obfuscated (
tutorial(# WatchID BIGINT NOT NULL,
tutorial(# JavaEnable SMALLINT NOT NULL,
tutorial(# Title TEXT NOT NULL,
tutorial(# GoodEvent SMALLINT NOT NULL,
tutorial(# EventTime TIMESTAMP NOT NULL,
tutorial(# EventDate Date NOT NULL,
tutorial(# CounterID INTEGER NOT NULL,
tutorial(# ClientIP INTEGER NOT NULL,
tutorial(# RegionID INTEGER NOT NULL,
tutorial(# UserID BIGINT NOT NULL,
tutorial(# CounterClass SMALLINT NOT NULL,
tutorial(# OS SMALLINT NOT NULL,
tutorial(# UserAgent SMALLINT NOT NULL,
tutorial(# URL TEXT NOT NULL,
tutorial(# Referer TEXT NOT NULL,
tutorial(# Refresh SMALLINT NOT NULL,
tutorial(# RefererCategoryID SMALLINT NOT NULL,
tutorial(# RefererRegionID INTEGER NOT NULL,
tutorial(# URLCategoryID SMALLINT NOT NULL,
tutorial(# URLRegionID INTEGER NOT NULL,
tutorial(# ResolutionWidth SMALLINT NOT NULL,
tutorial(# ResolutionHeight SMALLINT NOT NULL,
tutorial(# ResolutionDepth SMALLINT NOT NULL,
tutorial(# FlashMajor SMALLINT NOT NULL,
tutorial(# FlashMinor SMALLINT NOT NULL,
tutorial(# FlashMinor2 TEXT NOT NULL,
tutorial(# NetMajor SMALLINT NOT NULL,
tutorial(# NetMinor SMALLINT NOT NULL,
tutorial(# UserAgentMajor SMALLINT NOT NULL,
tutorial(# UserAgentMinor CHAR(2) NOT NULL,
tutorial(# CookieEnable SMALLINT NOT NULL,
tutorial(# JavascriptEnable SMALLINT NOT NULL,
tutorial(# IsMobile SMALLINT NOT NULL,
tutorial(# MobilePhone SMALLINT NOT NULL,
tutorial(# MobilePhoneModel TEXT NOT NULL,
tutorial(# Params TEXT NOT NULL,
tutorial(# IPNetworkID INTEGER NOT NULL,
tutorial(# TraficSourceID SMALLINT NOT NULL,
tutorial(# SearchEngineID SMALLINT NOT NULL,
tutorial(# SearchPhrase TEXT NOT NULL,
tutorial(# AdvEngineID SMALLINT NOT NULL,
tutorial(# IsArtifical SMALLINT NOT NULL,
tutorial(# WindowClientWidth SMALLINT NOT NULL,
tutorial(# WindowClientHeight SMALLINT NOT NULL,
tutorial(# ClientTimeZone SMALLINT NOT NULL,
tutorial(# ClientEventTime TIMESTAMP NOT NULL,
tutorial(# SilverlightVersion1 SMALLINT NOT NULL,
tutorial(# SilverlightVersion2 SMALLINT NOT NULL,
tutorial(# SilverlightVersion3 INTEGER NOT NULL,
tutorial(# SilverlightVersion4 SMALLINT NOT NULL,
tutorial(# PageCharset TEXT NOT NULL,
tutorial(# CodeVersion INTEGER NOT NULL,
tutorial(# IsLink SMALLINT NOT NULL,
tutorial(# IsDownload SMALLINT NOT NULL,
tutorial(# IsNotBounce SMALLINT NOT NULL,
tutorial(# FUniqID BIGINT NOT NULL,
tutorial(# OriginalURL TEXT NOT NULL,
tutorial(# HID INTEGER NOT NULL,
tutorial(# IsOldCounter SMALLINT NOT NULL,
tutorial(# IsEvent SMALLINT NOT NULL,
tutorial(# IsParameter SMALLINT NOT NULL,
tutorial(# DontCountHits SMALLINT NOT NULL,
tutorial(# WithHash SMALLINT NOT NULL,
tutorial(# HitColor CHAR NOT NULL,
tutorial(# LocalEventTime TIMESTAMP NOT NULL,
tutorial(# Age SMALLINT NOT NULL,
tutorial(# Sex SMALLINT NOT NULL,
tutorial(# Income SMALLINT NOT NULL,
tutorial(# Interests SMALLINT NOT NULL,
tutorial(# Robotness SMALLINT NOT NULL,
tutorial(# RemoteIP INTEGER NOT NULL,
tutorial(# WindowName INTEGER NOT NULL,
tutorial(# OpenerName INTEGER NOT NULL,
tutorial(# HistoryLength SMALLINT NOT NULL,
tutorial(# BrowserLanguage TEXT NOT NULL,
tutorial(# BrowserCountry TEXT NOT NULL,
tutorial(# SocialNetwork TEXT NOT NULL,
tutorial(# SocialAction TEXT NOT NULL,
tutorial(# HTTPError SMALLINT NOT NULL,
tutorial(# SendTiming INTEGER NOT NULL,
tutorial(# DNSTiming INTEGER NOT NULL,
tutorial(# ConnectTiming INTEGER NOT NULL,
tutorial(# ResponseStartTiming INTEGER NOT NULL,
tutorial(# ResponseEndTiming INTEGER NOT NULL,
tutorial(# FetchTiming INTEGER NOT NULL,
tutorial(# SocialSourceNetworkID SMALLINT NOT NULL,
tutorial(# SocialSourcePage TEXT NOT NULL,
tutorial(# ParamPrice BIGINT NOT NULL,
tutorial(# ParamOrderID TEXT NOT NULL,
tutorial(# ParamCurrency TEXT NOT NULL,
tutorial(# ParamCurrencyID SMALLINT NOT NULL,
tutorial(# OpenstatServiceName TEXT NOT NULL,
tutorial(# OpenstatCampaignID TEXT NOT NULL,
tutorial(# OpenstatAdID TEXT NOT NULL,
tutorial(# OpenstatSourceID TEXT NOT NULL,
tutorial(# UTMSource TEXT NOT NULL,
tutorial(# UTMMedium TEXT NOT NULL,
tutorial(# UTMCampaign TEXT NOT NULL,
tutorial(# UTMContent TEXT NOT NULL,
tutorial(# UTMTerm TEXT NOT NULL,
tutorial(# FromTag TEXT NOT NULL,
tutorial(# HasGCLID SMALLINT NOT NULL,
tutorial(# RefererHash BIGINT NOT NULL,
tutorial(# URLHash BIGINT NOT NULL,
tutorial(# CLID INTEGER NOT NULL
tutorial(# );
CREATE TABLE
tutorial=# SELECT create_hypertable('hits_100m_obfuscated', 'eventtime');
         create_hypertable
-----------------------------------
 (2,public,hits_100m_obfuscated,t)
(1 row)

tutorial=#
```

Now ok.


## Loading Data

Next - importing data:
https://docs.timescale.com/timescaledb/latest/how-to-guides/migrate-data/import-csv/#csv-import

```
SELECT WatchID::Int64, JavaEnable, toValidUTF8(Title), GoodEvent, EventTime, EventDate, CounterID::Int32, ClientIP::Int32, RegionID::Int32, UserID::Int64, CounterClass, OS, UserAgent, toValidUTF8(URL), toValidUTF8(Referer), Refresh, RefererCategoryID::Int16, RefererRegionID::Int32, URLCategoryID::Int16, URLRegionID::Int32, ResolutionWidth::Int16, ResolutionHeight::Int16, ResolutionDepth, FlashMajor, FlashMinor, FlashMinor2, NetMajor, NetMinor, UserAgentMajor::Int16, UserAgentMinor, CookieEnable, JavascriptEnable, IsMobile, MobilePhone, toValidUTF8(MobilePhoneModel), toValidUTF8(Params), IPNetworkID::Int32, TraficSourceID, SearchEngineID::Int16, toValidUTF8(SearchPhrase), AdvEngineID, IsArtifical, WindowClientWidth::Int16, WindowClientHeight::Int16, ClientTimeZone, ClientEventTime, SilverlightVersion1, SilverlightVersion2, SilverlightVersion3::Int32, SilverlightVersion4::Int16, toValidUTF8(PageCharset), CodeVersion::Int32, IsLink, IsDownload, IsNotBounce, FUniqID::Int64, toValidUTF8(OriginalURL), HID::Int32, IsOldCounter, IsEvent, IsParameter, DontCountHits, WithHash, HitColor, LocalEventTime, Age, Sex, Income, Interests::Int16, Robotness, RemoteIP::Int32, WindowName, OpenerName, HistoryLength, BrowserLanguage, BrowserCountry, toValidUTF8(SocialNetwork), toValidUTF8(SocialAction), HTTPError, SendTiming, DNSTiming, ConnectTiming, ResponseStartTiming, ResponseEndTiming, FetchTiming, SocialSourceNetworkID, toValidUTF8(SocialSourcePage), ParamPrice, toValidUTF8(ParamOrderID), ParamCurrency, ParamCurrencyID::Int16, OpenstatServiceName, OpenstatCampaignID, OpenstatAdID, OpenstatSourceID, UTMSource, UTMMedium, UTMCampaign, UTMContent, UTMTerm, FromTag, HasGCLID, RefererHash::Int64, URLHash::Int64, CLID::Int32
FROM hits_100m_obfuscated
INTO OUTFILE 'dump.csv'
FORMAT CSV
```

https://github.com/ClickHouse/ClickHouse/issues/30872
https://github.com/ClickHouse/ClickHouse/issues/30873

```
$ wc -c dump.csv
80865718769 dump.csv
```

```
milovidov@mtlog-perftest03j:~$ timescaledb-parallel-copy --db-name tutorial --table hits_100m_obfuscated --file dump.csv --workers 16 --copy-options "CSV"
panic: could not connect: pq: password authentication failed for user "postgres"

goroutine 12 [running]:
main.processBatches(0xc00001e3c0, 0xc0000a66c0)
        /home/builder/go/src/github.com/timescale/timescaledb-parallel-copy/cmd/timescaledb-parallel-copy/main.go:238 +0x887
created by main.main
        /home/builder/go/src/github.com/timescale/timescaledb-parallel-copy/cmd/timescaledb-parallel-copy/main.go:148 +0x1bb
milovidov@mtlog-perftest03j:~$ sudo -u postgres timescaledb-parallel-copy --db-name tutorial --table hits_100m_obfuscated --file dump.csv --workers 16 --copy-options "CSV"
panic: could not connect: pq: password authentication failed for user "postgres"

goroutine 25 [running]:
main.processBatches(0xc00019a350, 0xc00019e660)
        /home/builder/go/src/github.com/timescale/timescaledb-parallel-copy/cmd/timescaledb-parallel-copy/main.go:238 +0x887
created by main.main
        /home/builder/go/src/github.com/timescale/timescaledb-parallel-copy/cmd/timescaledb-parallel-copy/main.go:148 +0x1bb


milovidov@mtlog-perftest03j:~$ sudo -u postgres timescaledb-parallel-copy --db-name tutorial --table hits_100m_obfuscated --file dump.csv --workers 16 --copy-options "CSV" --host localhost
flag provided but not defined: -host
Usage of timescaledb-parallel-copy:
  -batch-size int
        Number of rows per insert (default 5000)
  -columns string
        Comma-separated columns present in CSV
  -connection string
        PostgreSQL connection url (default "host=localhost user=postgres sslmode=disable")
  -copy-options string
        Additional options to pass to COPY (e.g., NULL 'NULL') (default "CSV")
  -db-name string
        Database where the destination table exists
  -file string
        File to read from rather than stdin
  -header-line-count int
        Number of header lines (default 1)
  -limit int
        Number of rows to insert overall; 0 means to insert all
  -log-batches
        Whether to time individual batches.
  -reporting-period duration
        Period to report insert stats; if 0s, intermediate results will not be reported
  -schema string
        Destination table's schema (default "public")
  -skip-header
        Skip the first line of the input
  -split string
        Character to split by (default ",")
  -table string
        Destination table for insertions (default "test_table")
  -token-size int
        Maximum size to use for tokens. By default, this is 64KB, so any value less than that will be ignored (default 65536)
  -truncate
        Truncate the destination table before insert
  -verbose
        Print more information about copying statistics
  -version
        Show the version of this tool
  -workers int
        Number of parallel requests to make (default 1)


milovidov@mtlog-perftest03j:~$ sudo -u postgres timescaledb-parallel-copy --db-name tutorial --table hits_100m_obfuscated --file dump.csv --workers 16 --copy-options "CSV" -connection 'host=localhost'
panic: could not connect: pq: password authentication failed for user "postgres"

goroutine 14 [running]:
main.processBatches(0xc0000183d0, 0xc0000a66c0)
        /home/builder/go/src/github.com/timescale/timescaledb-parallel-copy/cmd/timescaledb-parallel-copy/main.go:238 +0x887
created by main.main
        /home/builder/go/src/github.com/timescale/timescaledb-parallel-copy/cmd/timescaledb-parallel-copy/main.go:148 +0x1bb
panic: could not connect: pq: password authentication failed for user "postgres"

goroutine 13 [running]:
main.processBatches(0xc0000183d0, 0xc0000a66c0)
        /home/builder/go/src/github.com/timescale/timescaledb-parallel-copy/cmd/timescaledb-parallel-copy/main.go:238 +0x887
created by main.main
        /home/builder/go/src/github.com/timescale/timescaledb-parallel-copy/cmd/timescaledb-parallel-copy/main.go:148 +0x1bb
panic: could not connect: pq: password authentication failed for user "postgres"

goroutine 12 [running]:
main.processBatches(0xc0000183d0, 0xc0000a66c0)
        /home/builder/go/src/github.com/timescale/timescaledb-parallel-copy/cmd/timescaledb-parallel-copy/main.go:238 +0x887
created by main.main
        /home/builder/go/src/github.com/timescale/timescaledb-parallel-copy/cmd/timescaledb-parallel-copy/main.go:148 +0x1bb


milovidov@mtlog-perftest03j:~$ sudo -u postgres timescaledb-parallel-copy --db-name tutorial --table hits_100m_obfuscated --file dump.csv --workers 16 --copy-options "CSV" -connection 'host=localhost password 12345'
panic: could not connect: cannot parse `host=localhost password 12345`: failed to parse as DSN (invalid dsn)

goroutine 13 [running]:
main.processBatches(0xc0000183d0, 0xc0000a66c0)
        /home/builder/go/src/github.com/timescale/timescaledb-parallel-copy/cmd/timescaledb-parallel-copy/main.go:238 +0x887
created by main.main
        /home/builder/go/src/github.com/timescale/timescaledb-parallel-copy/cmd/timescaledb-parallel-copy/main.go:148 +0x1bb


milovidov@mtlog-perftest03j:~$ sudo -u postgres timescaledb-parallel-copy --db-name tutorial --table hits_100m_obfuscated --file dump.csv --workers 16 --copy-options "CSV" -connection 'host=localhost password=12345'
panic: pq: invalid byte sequence for encoding "UTF8": 0xe0 0x22 0x2c

goroutine 34 [running]:
main.processBatches(0xc000132350, 0xc000136660)
        /home/builder/go/src/github.com/timescale/timescaledb-parallel-copy/cmd/timescaledb-parallel-copy/main.go:262 +0x879
created by main.main
        /home/builder/go/src/github.com/timescale/timescaledb-parallel-copy/cmd/timescaledb-parallel-copy/main.go:148 +0x1bb
panic: pq: invalid byte sequence for encoding "UTF8": 0xe0 0x22 0x2c

goroutine 30 [running]:
main.processBatches(0xc000132350, 0xc000136660)
        /home/builder/go/src/github.com/timescale/timescaledb-parallel-copy/cmd/timescaledb-parallel-copy/main.go:262 +0x879
created by main.main
        /home/builder/go/src/github.com/timescale/timescaledb-parallel-copy/cmd/timescaledb-parallel-copy/main.go:148 +0x1bb
```

Ok, now I've got something meaningful.
But it does not show, what line has error...

```
$ echo -e '\xe0\x22\x2c'
�",
```

Let's recreate the dump:

```
rm dump.csv

SELECT WatchID::Int64, JavaEnable, toValidUTF8(Title), GoodEvent, EventTime, EventDate, CounterID::Int32, ClientIP::Int32, RegionID::Int32,
    UserID::Int64, CounterClass, OS, UserAgent, toValidUTF8(URL), toValidUTF8(Referer), Refresh, RefererCategoryID::Int16, RefererRegionID::Int32,
    URLCategoryID::Int16, URLRegionID::Int32, ResolutionWidth::Int16, ResolutionHeight::Int16, ResolutionDepth, FlashMajor, FlashMinor,
    FlashMinor2, NetMajor, NetMinor, UserAgentMajor::Int16, toValidUTF8(UserAgentMinor::String), CookieEnable, JavascriptEnable, IsMobile, MobilePhone,
    toValidUTF8(MobilePhoneModel), toValidUTF8(Params), IPNetworkID::Int32, TraficSourceID, SearchEngineID::Int16, toValidUTF8(SearchPhrase),
    AdvEngineID, IsArtifical, WindowClientWidth::Int16, WindowClientHeight::Int16, ClientTimeZone, ClientEventTime,
    SilverlightVersion1, SilverlightVersion2, SilverlightVersion3::Int32, SilverlightVersion4::Int16, toValidUTF8(PageCharset),
    CodeVersion::Int32, IsLink, IsDownload, IsNotBounce, FUniqID::Int64, toValidUTF8(OriginalURL), HID::Int32, IsOldCounter, IsEvent,
    IsParameter, DontCountHits, WithHash, toValidUTF8(HitColor::String), LocalEventTime, Age, Sex, Income, Interests::Int16, Robotness, RemoteIP::Int32,
    WindowName, OpenerName, HistoryLength, toValidUTF8(BrowserLanguage::String), toValidUTF8(BrowserCountry::String),
    toValidUTF8(SocialNetwork), toValidUTF8(SocialAction),
    HTTPError, SendTiming, DNSTiming, ConnectTiming, ResponseStartTiming, ResponseEndTiming, FetchTiming, SocialSourceNetworkID,
    toValidUTF8(SocialSourcePage), ParamPrice, toValidUTF8(ParamOrderID), toValidUTF8(ParamCurrency::String),
    ParamCurrencyID::Int16, OpenstatServiceName, OpenstatCampaignID, OpenstatAdID, OpenstatSourceID,
    UTMSource, UTMMedium, UTMCampaign, UTMContent, UTMTerm, FromTag, HasGCLID, RefererHash::Int64, URLHash::Int64, CLID::Int32
FROM hits_100m_obfuscated
INTO OUTFILE 'dump.csv'
FORMAT CSV
```

```
$ sudo -u postgres timescaledb-parallel-copy --db-name tutorial --table hits_100m_obfuscated --file dump.csv --workers 1 --copy-options "CSV" -connection 'host=localhost password=12345'
panic: pq: value too long for type character(2)

goroutine 6 [running]:
main.processBatches(0xc0000183d0, 0xc0000a66c0)
        /home/builder/go/src/github.com/timescale/timescaledb-parallel-copy/cmd/timescaledb-parallel-copy/main.go:262 +0x879
created by main.main
        /home/builder/go/src/github.com/timescale/timescaledb-parallel-copy/cmd/timescaledb-parallel-copy/main.go:148 +0x1bb
```

ALTER does not work:

```
tutorial=# ALTER TABLE hits_100m_obfuscated MODIFY COLUMN UserAgentMinor TEXT
tutorial-# ;
ERROR:  syntax error at or near "MODIFY"
LINE 1: ALTER TABLE hits_100m_obfuscated MODIFY COLUMN UserAgentMino...
                                         ^
```

PostgreSQL is using unusual syntax for ALTER:

```
tutorial=# ALTER TABLE hits_100m_obfuscated ALTER COLUMN UserAgentMinor TYPE TEXT
;
ALTER TABLE
tutorial=# \q
```

https://github.com/ClickHouse/ClickHouse/issues/30874

Now something again:

```
$ sudo -u postgres timescaledb-parallel-copy --db-name tutorial --table hits_100m_obfuscated --file dump.csv --workers 1 --copy-options "CSV" -connection 'host=localhost password=12345'
panic: pq: value "2149615427" is out of range for type integer

goroutine 6 [running]:
main.processBatches(0xc0000183d0, 0xc0000a66c0)
        /home/builder/go/src/github.com/timescale/timescaledb-parallel-copy/cmd/timescaledb-parallel-copy/main.go:262 +0x879
created by main.main
        /home/builder/go/src/github.com/timescale/timescaledb-parallel-copy/cmd/timescaledb-parallel-copy/main.go:148 +0x1bb
```

```
$ grep -F '2149615427' dump.csv
5607505572457935073,0,"Лазар автоматические пылесосы подробная школы. Когалерея — Курсы на Автория пище Сноудента новые устами",1,"2013-07-15 07:47:45","2013-07-15",38,-1194330980,229,-6649844357037090659,0,2,3,"https://produkty%2Fkategory_id=&auto-nexus.html?blockfesty-i-korroszhego","http://tambov.irr.ua/yandex.ru/saledParam=0&user/auto.ria",1,10282,995,15014,519,1996,1781,23,14,2,"800",0,0,7,"D�",1,1,0,0,"","",3392210,-1,0,"",0,0,1261,1007,135,"2013-07-15 21:54:13",0,0,0,0,"windows-1251;charset",1601,0,0,0,8184671896482443026,"",451733382,0,0,0,0,0,"5","2013-07-15 15:41:14",31,1,3,60,13,-1855237933,-1,-1,-1,"S0","h1","","",0,0,0,0,2149615427,36,3,0,"",0,"","NH",0,"","","","","","","","","","",0,-1103774879459415602,-2414747266057209563,0
^C
```

Let's recreate the dump:

```
rm dump.csv

SELECT WatchID::Int64, JavaEnable, toValidUTF8(Title), GoodEvent, EventTime, EventDate, CounterID::Int32, ClientIP::Int32, RegionID::Int32,
    UserID::Int64, CounterClass, OS, UserAgent, toValidUTF8(URL), toValidUTF8(Referer), Refresh, RefererCategoryID::Int16, RefererRegionID::Int32,
    URLCategoryID::Int16, URLRegionID::Int32, ResolutionWidth::Int16, ResolutionHeight::Int16, ResolutionDepth, FlashMajor, FlashMinor,
    FlashMinor2, NetMajor, NetMinor, UserAgentMajor::Int16, toValidUTF8(UserAgentMinor::String), CookieEnable, JavascriptEnable, IsMobile, MobilePhone,
    toValidUTF8(MobilePhoneModel), toValidUTF8(Params), IPNetworkID::Int32, TraficSourceID, SearchEngineID::Int16, toValidUTF8(SearchPhrase),
    AdvEngineID, IsArtifical, WindowClientWidth::Int16, WindowClientHeight::Int16, ClientTimeZone, ClientEventTime,
    SilverlightVersion1, SilverlightVersion2, SilverlightVersion3::Int32, SilverlightVersion4::Int16, toValidUTF8(PageCharset),
    CodeVersion::Int32, IsLink, IsDownload, IsNotBounce, FUniqID::Int64, toValidUTF8(OriginalURL), HID::Int32, IsOldCounter, IsEvent,
    IsParameter, DontCountHits, WithHash, toValidUTF8(HitColor::String), LocalEventTime, Age, Sex, Income, Interests::Int16, Robotness, RemoteIP::Int32,
    WindowName, OpenerName, HistoryLength, toValidUTF8(BrowserLanguage::String), toValidUTF8(BrowserCountry::String),
    toValidUTF8(SocialNetwork), toValidUTF8(SocialAction),
    HTTPError, least(SendTiming, 30000), least(DNSTiming, 30000), least(ConnectTiming, 30000), least(ResponseStartTiming, 30000),
    least(ResponseEndTiming, 30000), least(FetchTiming, 30000), SocialSourceNetworkID,
    toValidUTF8(SocialSourcePage), ParamPrice, toValidUTF8(ParamOrderID), toValidUTF8(ParamCurrency::String),
    ParamCurrencyID::Int16, OpenstatServiceName, OpenstatCampaignID, OpenstatAdID, OpenstatSourceID,
    UTMSource, UTMMedium, UTMCampaign, UTMContent, UTMTerm, FromTag, HasGCLID, RefererHash::Int64, URLHash::Int64, CLID::Int32
FROM hits_100m_obfuscated
INTO OUTFILE 'dump.csv'
FORMAT CSV
```

PostgreSQL does not support USE database.
But I remember, that I can write `\c` instead. I guess `\c` means "change" (the database). Or it is called "schema" or "catalog".

```
milovidov@mtlog-perftest03j:~$ sudo -u postgres psql
psql (13.4 (Ubuntu 13.4-4.pgdg18.04+1))
Type "help" for help.

postgres=# SELECT count(*) FROM hits_100m_obfuscated;
ERROR:  relation "hits_100m_obfuscated" does not exist
LINE 1: SELECT count(*) FROM hits_100m_obfuscated;
                             ^
postgres=# USE tutorial;
ERROR:  syntax error at or near "USE"
LINE 1: USE tutorial;
        ^
postgres=# \c tutorial
You are now connected to database "tutorial" as user "postgres".
tutorial=# SELECT count(*) FROM hits_100m_obfuscated;
 count
-------
 69996
(1 row)
```

And parallel loader already loaded some part of data into my table (it is not transactional).
Let's truncate table:

```
tutorial=# TRUNCATE TABLE hits_100m_obfuscated;
TRUNCATE TABLE
```

Surprisingly, it works!

Now it started loading data:
```
$ time sudo -u postgres timescaledb-parallel-copy --db-name tutorial --table hits_100m_obfuscated --file dump.csv --workers 16 --copy-options "CSV" -connection 'host=localhost password=12345'
```

But the loading is not using 16 CPU cores and it is not bottlenecked by IO.

WTF:

```
$ time sudo -u postgres timescaledb-parallel-copy --db-name tutorial --table hits_100m_obfuscated --file dump.csv --workers 16 --copy-options "CSV" -connection 'host=localhost password=12345'
panic: pq: could not extend file "base/16384/31264.1": wrote only 4096 of 8192 bytes at block 145407

goroutine 6 [running]:
main.processBatches(0xc0000183d0, 0xc0000a66c0)
        /home/builder/go/src/github.com/timescale/timescaledb-parallel-copy/cmd/timescaledb-parallel-copy/main.go:262 +0x879
created by main.main
        /home/builder/go/src/github.com/timescale/timescaledb-parallel-copy/cmd/timescaledb-parallel-copy/main.go:148 +0x1bb

real    3m31.328s
user    0m35.016s
sys     0m6.964s
```

Looks like there is no space:

```
milovidov@mtlog-perftest03j:~$ df -h /var/lib/postgresql/13/main
Filesystem      Size  Used Avail Use% Mounted on
/dev/md1         35G   33G  1.4G  97% /
```

https://github.com/ClickHouse/ClickHouse/issues/30883

Let's move to another device.

```
milovidov@mtlog-perftest03j:~$ sudo mkdir /opt/postgresql
milovidov@mtlog-perftest03j:~$ sudo ls -l /var/lib/postgresql/13/main
total 88
drwx------ 6 postgres postgres  4096 Oct 30 00:06 base
drwx------ 2 postgres postgres  4096 Oct 30 02:07 global
drwx------ 2 postgres postgres  4096 Oct 29 23:27 pg_commit_ts
drwx------ 2 postgres postgres  4096 Oct 29 23:27 pg_dynshmem
drwx------ 4 postgres postgres  4096 Oct 30 02:10 pg_logical
drwx------ 4 postgres postgres  4096 Oct 29 23:27 pg_multixact
drwx------ 2 postgres postgres  4096 Oct 29 23:27 pg_notify
drwx------ 2 postgres postgres  4096 Oct 29 23:27 pg_replslot
drwx------ 2 postgres postgres  4096 Oct 29 23:27 pg_serial
drwx------ 2 postgres postgres  4096 Oct 29 23:27 pg_snapshots
drwx------ 2 postgres postgres  4096 Oct 30 02:10 pg_stat
drwx------ 2 postgres postgres  4096 Oct 29 23:27 pg_stat_tmp
drwx------ 2 postgres postgres  4096 Oct 29 23:27 pg_subtrans
drwx------ 2 postgres postgres  4096 Oct 29 23:27 pg_tblspc
drwx------ 2 postgres postgres  4096 Oct 29 23:27 pg_twophase
-rw------- 1 postgres postgres     3 Oct 29 23:27 PG_VERSION
drwx------ 3 postgres postgres 12288 Oct 30 02:10 pg_wal
drwx------ 2 postgres postgres  4096 Oct 29 23:27 pg_xact
-rw------- 1 postgres postgres    88 Oct 29 23:27 postgresql.auto.conf
-rw------- 1 postgres postgres   130 Oct 30 00:03 postmaster.opts
milovidov@mtlog-perftest03j:~$ sudo chown postgres:postgres /opt/postgresql
milovidov@mtlog-perftest03j:~$ sudo mv /var/lib/postgresql/13/main/* /opt/postgresql
mv: cannot stat '/var/lib/postgresql/13/main/*': No such file or directory
milovidov@mtlog-perftest03j:~$ sudo bash -c 'mv /var/lib/postgresql/13/main/* /opt/postgresql'
sudo ln milovidov@mtlog-perftest03j:~$ #sudo ln -s /opt/postgresql /var/lib/postgresql/13/main
milovidov@mtlog-perftest03j:~$ sudo rm /var/lib/postgresql/13/main
rm: cannot remove '/var/lib/postgresql/13/main': Is a directory
milovidov@mtlog-perftest03j:~$ sudo rm -rf /var/lib/postgresql/13/main
milovidov@mtlog-perftest03j:~$ sudo ln -s /opt/postgresql /var/lib/postgresql/13/main
milovidov@mtlog-perftest03j:~$ sudo ls -l /var/lib/postgresql/13/main
lrwxrwxrwx 1 root root 15 Oct 30 02:12 /var/lib/postgresql/13/main -> /opt/postgresql
milovidov@mtlog-perftest03j:~$ sudo ls -l /opt/postgresql/
total 80
drwx------ 6 postgres postgres 4096 Oct 30 00:06 base
drwx------ 2 postgres postgres 4096 Oct 30 02:07 global
drwx------ 2 postgres postgres 4096 Oct 29 23:27 pg_commit_ts
drwx------ 2 postgres postgres 4096 Oct 29 23:27 pg_dynshmem
drwx------ 4 postgres postgres 4096 Oct 30 02:10 pg_logical
drwx------ 4 postgres postgres 4096 Oct 29 23:27 pg_multixact
drwx------ 2 postgres postgres 4096 Oct 29 23:27 pg_notify
drwx------ 2 postgres postgres 4096 Oct 29 23:27 pg_replslot
drwx------ 2 postgres postgres 4096 Oct 29 23:27 pg_serial
drwx------ 2 postgres postgres 4096 Oct 29 23:27 pg_snapshots
drwx------ 2 postgres postgres 4096 Oct 30 02:10 pg_stat
drwx------ 2 postgres postgres 4096 Oct 29 23:27 pg_stat_tmp
drwx------ 2 postgres postgres 4096 Oct 29 23:27 pg_subtrans
drwx------ 2 postgres postgres 4096 Oct 29 23:27 pg_tblspc
drwx------ 2 postgres postgres 4096 Oct 29 23:27 pg_twophase
-rw------- 1 postgres postgres    3 Oct 29 23:27 PG_VERSION
drwx------ 3 postgres postgres 4096 Oct 30 02:10 pg_wal
drwx------ 2 postgres postgres 4096 Oct 29 23:27 pg_xact
-rw------- 1 postgres postgres   88 Oct 29 23:27 postgresql.auto.conf
-rw------- 1 postgres postgres  130 Oct 30 00:03 postmaster.opts

sudo service postgresql start

sudo less /var/log/postgresql/postgresql-13-main.log

2021-10-30 02:13:41.284 MSK [791362] FATAL:  data directory "/var/lib/postgresql/13/main" has invalid permissions
2021-10-30 02:13:41.284 MSK [791362] DETAIL:  Permissions should be u=rwx (0700) or u=rwx,g=rx (0750).
pg_ctl: could not start server
Examine the log output.

sudo chmod 0700 /var/lib/postgresql/13/main /opt/postgresql
sudo service postgresql start

postgres=# \c tutorial
You are now connected to database "tutorial" as user "postgres".
tutorial=# TRUNCATE TABLE hits_100m_obfuscated;
TRUNCATE TABLE
```

```
$ time sudo -u postgres timescaledb-parallel-copy --db-name tutorial --table hits_100m_obfuscated --file dump.csv --workers 16 --copy-options "CSV" -connection 'host=localhost password=12345'
```

No success:

```
$ time sudo -u postgres timescaledb-parallel-copy --db-name tutorial --table hits_100m_obfuscated --file dump.csv --workers 16 --copy-options "CSV" -connection 'host=localhost password=12345'
panic: pq: invalid byte sequence for encoding "UTF8": 0x00

goroutine 29 [running]:
main.processBatches(0xc000132350, 0xc000136660)
        /home/builder/go/src/github.com/timescale/timescaledb-parallel-copy/cmd/timescaledb-parallel-copy/main.go:262 +0x879
created by main.main
        /home/builder/go/src/github.com/timescale/timescaledb-parallel-copy/cmd/timescaledb-parallel-copy/main.go:148 +0x1bb

real    11m47.879s
user    3m10.980s
sys     0m45.256s
```

The error message is false, because UTF-8 **does** support 0x00. It is just some PostgreSQL quirk.

Let's recreate the dump:

```
rm dump.csv

SELECT WatchID::Int64, JavaEnable, replaceAll(toValidUTF8(Title), '\0', ''), GoodEvent, EventTime, EventDate, CounterID::Int32, ClientIP::Int32, RegionID::Int32,
    UserID::Int64, CounterClass, OS, UserAgent, replaceAll(toValidUTF8(URL), '\0', ''), replaceAll(toValidUTF8(Referer), '\0', ''), Refresh, RefererCategoryID::Int16, RefererRegionID::Int32,
    URLCategoryID::Int16, URLRegionID::Int32, ResolutionWidth::Int16, ResolutionHeight::Int16, ResolutionDepth, FlashMajor, FlashMinor,
    FlashMinor2, NetMajor, NetMinor, UserAgentMajor::Int16, replaceAll(toValidUTF8(UserAgentMinor::String), '\0', ''), CookieEnable, JavascriptEnable, IsMobile, MobilePhone,
    replaceAll(toValidUTF8(MobilePhoneModel), '\0', ''), replaceAll(toValidUTF8(Params), '\0', ''), IPNetworkID::Int32, TraficSourceID, SearchEngineID::Int16, replaceAll(toValidUTF8(SearchPhrase), '\0', ''),
    AdvEngineID, IsArtifical, WindowClientWidth::Int16, WindowClientHeight::Int16, ClientTimeZone, ClientEventTime,
    SilverlightVersion1, SilverlightVersion2, SilverlightVersion3::Int32, SilverlightVersion4::Int16, replaceAll(toValidUTF8(PageCharset), '\0', ''),
    CodeVersion::Int32, IsLink, IsDownload, IsNotBounce, FUniqID::Int64, replaceAll(toValidUTF8(OriginalURL), '\0', ''), HID::Int32, IsOldCounter, IsEvent,
    IsParameter, DontCountHits, WithHash, replaceAll(toValidUTF8(HitColor::String), '\0', ''), LocalEventTime, Age, Sex, Income, Interests::Int16, Robotness, RemoteIP::Int32,
    WindowName, OpenerName, HistoryLength, replaceAll(toValidUTF8(BrowserLanguage::String), '\0', ''), replaceAll(toValidUTF8(BrowserCountry::String), '\0', ''),
    replaceAll(toValidUTF8(SocialNetwork), '\0', ''), replaceAll(toValidUTF8(SocialAction), '\0', ''),
    HTTPError, least(SendTiming, 30000), least(DNSTiming, 30000), least(ConnectTiming, 30000), least(ResponseStartTiming, 30000),
    least(ResponseEndTiming, 30000), least(FetchTiming, 30000), SocialSourceNetworkID,
    replaceAll(toValidUTF8(SocialSourcePage), '\0', ''), ParamPrice, replaceAll(toValidUTF8(ParamOrderID), '\0', ''), replaceAll(toValidUTF8(ParamCurrency::String), '\0', ''),
    ParamCurrencyID::Int16, OpenstatServiceName, OpenstatCampaignID, OpenstatAdID, OpenstatSourceID,
    UTMSource, UTMMedium, UTMCampaign, UTMContent, UTMTerm, FromTag, HasGCLID, RefererHash::Int64, URLHash::Int64, CLID::Int32
FROM hits_100m_obfuscated
INTO OUTFILE 'dump.csv'
FORMAT CSV
```

WTF:

```
tutorial=# SELECT count(*) FROM hits_100m_obfuscated;
ERROR:  could not load library "/usr/lib/postgresql/13/lib/llvmjit.so": libLLVM-6.0.so.1: cannot open shared object file: No such file or directory
```

Maybe just install LLVM?

```
sudo apt install llvm
```

It does not help:

```
milovidov@mtlog-perftest03j:~$ sudo -u postgres psql
psql (13.4 (Ubuntu 13.4-4.pgdg18.04+1))
Type "help" for help.

postgres=# \c tutorial
You are now connected to database "tutorial" as user "postgres".
tutorial=# SELECT count(*) FROM hits_100m_obfuscated;
ERROR:  could not load library "/usr/lib/postgresql/13/lib/llvmjit.so": libLLVM-6.0.so.1: cannot open shared object file: No such file or directory
tutorial=#
```

Dependency on system libraries is harmful.

```
milovidov@mtlog-perftest03j:~$ ls -l /usr/lib/x86_64-linux-gnu/libLLVM-6.0.so
lrwxrwxrwx 1 root root 16 Apr  6  2018 /usr/lib/x86_64-linux-gnu/libLLVM-6.0.so -> libLLVM-6.0.so.1
milovidov@mtlog-perftest03j:~$ ls -l /usr/lib/x86_64-linux-gnu/libLLVM-6.0.so.1
ls: cannot access '/usr/lib/x86_64-linux-gnu/libLLVM-6.0.so.1': No such file or directory
```

https://askubuntu.com/questions/481/how-do-i-find-the-package-that-provides-a-file

```
milovidov@mtlog-perftest03j:~$ dpkg -S libLLVM-6.0.so.1
llvm-6.0-dev: /usr/lib/llvm-6.0/lib/libLLVM-6.0.so.1
libllvm6.0:amd64: /usr/lib/x86_64-linux-gnu/libLLVM-6.0.so.1
```

Wow, it's absolutely broken:

```
milovidov@mtlog-perftest03j:~$ sudo apt remove llvm-6.0-dev
Reading package lists... Done
Building dependency tree
Reading state information... Done
The following packages were automatically installed and are no longer required:
  libcgal13 libgmpxx4ldbl liblldb-11 libprotobuf-c1 libsfcgal1 mysql-server-core-5.7
Use 'sudo apt autoremove' to remove them.
The following packages will be REMOVED:
  liblld-6.0-dev lld lld-6.0 llvm-6.0-dev
0 upgraded, 0 newly installed, 4 to remove and 293 not upgraded.
After this operation, 163 MB disk space will be freed.
Do you want to continue? [Y/n]
(Reading database ... 268641 files and directories currently installed.)
Removing liblld-6.0-dev (1:6.0-1ubuntu2) ...
Removing lld (1:6.0-41~exp5~ubuntu1) ...
Removing lld-6.0 (1:6.0-1ubuntu2) ...
Removing llvm-6.0-dev (1:6.0-1ubuntu2) ...
Processing triggers for man-db (2.8.3-2ubuntu0.1) ...
Processing triggers for libc-bin (2.27-3ubuntu1.4) ...
milovidov@mtlog-perftest03j:~$ sudo apt install llvm-6.0-dev
Reading package lists... Done
Building dependency tree
Reading state information... Done
The following packages were automatically installed and are no longer required:
  libcgal13 libgmpxx4ldbl liblldb-11 libprotobuf-c1 libsfcgal1 mysql-server-core-5.7
Use 'sudo apt autoremove' to remove them.
The following NEW packages will be installed:
  llvm-6.0-dev
0 upgraded, 1 newly installed, 0 to remove and 293 not upgraded.
Need to get 23.0 MB of archives.
After this operation, 160 MB of additional disk space will be used.
Get:1 http://mirror.yandex.ru/ubuntu bionic/main amd64 llvm-6.0-dev amd64 1:6.0-1ubuntu2 [23.0 MB]
Fetched 23.0 MB in 1s (42.5 MB/s)
Selecting previously unselected package llvm-6.0-dev.
(Reading database ... 267150 files and directories currently installed.)
Preparing to unpack .../llvm-6.0-dev_1%3a6.0-1ubuntu2_amd64.deb ...
Unpacking llvm-6.0-dev (1:6.0-1ubuntu2) ...
Setting up llvm-6.0-dev (1:6.0-1ubuntu2) ...
Processing triggers for libc-bin (2.27-3ubuntu1.4) ...
milovidov@mtlog-perftest03j:~$ ls -l /usr/lib/x86_64-linux-gnu/libLLVM-6.0.so
lrwxrwxrwx 1 root root 16 Apr  6  2018 /usr/lib/x86_64-linux-gnu/libLLVM-6.0.so -> libLLVM-6.0.so.1
milovidov@mtlog-perftest03j:~$ ls -l /usr/lib/x86_64-linux-gnu/libLLVM-6.0.so.1
ls: cannot access '/usr/lib/x86_64-linux-gnu/libLLVM-6.0.so.1': No such file or directory
```

Let's remove just in case:

```
sudo apt remove llvm-6.0-dev
```

https://dba.stackexchange.com/questions/264955/handling-performance-problems-with-jit-in-postgres-12

JIT can be disabled by `set jit = off;`

```
tutorial=# set jit = off;
SET
tutorial=#
tutorial=# SELECT count(*) FROM hits_100m_obfuscated;
```

But now this SELECT query started and hanged for multiple minutes without any result.
And I see something strange in `top`:

```
 792553 postgres  20   0 32.418g 0.031t 0.031t D   2.4 25.3   3:43.84 postgres: 13/main: checkpointer
 814659 postgres  20   0 32.432g 0.023t 0.023t D   0.0 18.8   0:14.53 postgres: 13/main: parallel worker for PID 813980
 813980 postgres  20   0 32.433g 0.023t 0.023t D   0.0 18.4   0:14.47 postgres: 13/main: postgres tutorial [local] SELECT
 814657 postgres  20   0 32.432g 0.016t 0.016t D   0.0 12.6   0:09.83 postgres: 13/main: parallel worker for PID 813980
 814658 postgres  20   0 32.432g 0.015t 0.015t D   2.4 12.6   0:09.45 postgres: 13/main: parallel worker for PID 813980
 814656 postgres  20   0 32.432g 0.015t 0.015t D   0.0 12.0   0:07.36 postgres: 13/main: parallel worker for PID 813980
 792554 postgres  20   0 32.417g 5.394g 5.392g D   0.0  4.3   0:04.78 postgres: 13/main: background writer
```

The query did not finish in 30 minutes. How it can be so enormously slow?


Loading failed, again:

```
$ time sudo -u postgres timescaledb-parallel-copy --db-name tutorial --table hits_100m_obfuscated --file dump.csv --workers 16 --copy-options "CSV" -connection 'host=localhost password=12345'
panic: pq: extra data after last expected column

goroutine 14 [running]:
main.processBatches(0xc0000183d0, 0xc0000a66c0)
        /home/builder/go/src/github.com/timescale/timescaledb-parallel-copy/cmd/timescaledb-parallel-copy/main.go:262 +0x879
created by main.main
        /home/builder/go/src/github.com/timescale/timescaledb-parallel-copy/cmd/timescaledb-parallel-copy/main.go:148 +0x1bb

real    20m57.936s
user    4m14.444s
sys     1m11.412s
```

Most likely PostgreSQL cannot recognize proper CSV escaping of quotes like `"Hello "" world"`.
Let's simply remove all double quotes from String values.

```
rm dump.csv

SELECT WatchID::Int64, JavaEnable, replaceAll(replaceAll(toValidUTF8(Title), '\0', ''), '"', ''), GoodEvent, EventTime, EventDate, CounterID::Int32, ClientIP::Int32, RegionID::Int32,
    UserID::Int64, CounterClass, OS, UserAgent, replaceAll(replaceAll(toValidUTF8(URL), '\0', ''), '"', ''), replaceAll(replaceAll(toValidUTF8(Referer), '\0', ''), '"', ''), Refresh, RefererCategoryID::Int16, RefererRegionID::Int32,
    URLCategoryID::Int16, URLRegionID::Int32, ResolutionWidth::Int16, ResolutionHeight::Int16, ResolutionDepth, FlashMajor, FlashMinor,
    FlashMinor2, NetMajor, NetMinor, UserAgentMajor::Int16, replaceAll(replaceAll(toValidUTF8(UserAgentMinor::String), '\0', ''), '"', ''), CookieEnable, JavascriptEnable, IsMobile, MobilePhone,
    replaceAll(replaceAll(toValidUTF8(MobilePhoneModel), '\0', ''), '"', ''), replaceAll(replaceAll(toValidUTF8(Params), '\0', ''), '"', ''), IPNetworkID::Int32, TraficSourceID, SearchEngineID::Int16, replaceAll(replaceAll(toValidUTF8(SearchPhrase), '\0', ''), '"', ''),
    AdvEngineID, IsArtifical, WindowClientWidth::Int16, WindowClientHeight::Int16, ClientTimeZone, ClientEventTime,
    SilverlightVersion1, SilverlightVersion2, SilverlightVersion3::Int32, SilverlightVersion4::Int16, replaceAll(replaceAll(toValidUTF8(PageCharset), '\0', ''), '"', ''),
    CodeVersion::Int32, IsLink, IsDownload, IsNotBounce, FUniqID::Int64, replaceAll(replaceAll(toValidUTF8(OriginalURL), '\0', ''), '"', ''), HID::Int32, IsOldCounter, IsEvent,
    IsParameter, DontCountHits, WithHash, replaceAll(replaceAll(toValidUTF8(HitColor::String), '\0', ''), '"', ''), LocalEventTime, Age, Sex, Income, Interests::Int16, Robotness, RemoteIP::Int32,
    WindowName, OpenerName, HistoryLength, replaceAll(replaceAll(toValidUTF8(BrowserLanguage::String), '\0', ''), '"', ''), replaceAll(replaceAll(toValidUTF8(BrowserCountry::String), '\0', ''), '"', ''),
    replaceAll(replaceAll(toValidUTF8(SocialNetwork), '\0', ''), '"', ''), replaceAll(replaceAll(toValidUTF8(SocialAction), '\0', ''), '"', ''),
    HTTPError, least(SendTiming, 30000), least(DNSTiming, 30000), least(ConnectTiming, 30000), least(ResponseStartTiming, 30000),
    least(ResponseEndTiming, 30000), least(FetchTiming, 30000), SocialSourceNetworkID,
    replaceAll(replaceAll(toValidUTF8(SocialSourcePage), '\0', ''), '"', ''), ParamPrice, replaceAll(replaceAll(toValidUTF8(ParamOrderID), '\0', ''), '"', ''), replaceAll(replaceAll(toValidUTF8(ParamCurrency::String), '\0', ''), '"', ''),
    ParamCurrencyID::Int16, OpenstatServiceName, OpenstatCampaignID, OpenstatAdID, OpenstatSourceID,
    UTMSource, UTMMedium, UTMCampaign, UTMContent, UTMTerm, FromTag, HasGCLID, RefererHash::Int64, URLHash::Int64, CLID::Int32
FROM hits_100m_obfuscated
INTO OUTFILE 'dump.csv'
FORMAT CSV
```

Oops, another trouble:

```
$ time sudo -u postgres timescaledb-parallel-copy --db-name tutorial --table hits_100m_obfuscated --file dump.csv --workers 16 --copy-options "CSV" -connection 'host=localhost password=12345'
panic: pq: unterminated CSV quoted field

goroutine 19 [running]:
main.processBatches(0xc000132350, 0xc000136660)
        /home/builder/go/src/github.com/timescale/timescaledb-parallel-copy/cmd/timescaledb-parallel-copy/main.go:262 +0x879
created by main.main
        /home/builder/go/src/github.com/timescale/timescaledb-parallel-copy/cmd/timescaledb-parallel-copy/main.go:148 +0x1bb

real    0m38.278s
user    0m13.544s
sys     0m3.552s
```

I have hypothesis, maybe it is interpreting both backslashes and quotes in CSV?
We need to check, what is CSV, exactly, from TimescaleDB's standpoint.

https://www.postgresql.org/docs/9.2/sql-copy.html

Yes, PostgreSQL is using "fake CSV":

> This format option is used for importing and exporting the Comma Separated Value (CSV) file format used by many other programs, such as spreadsheets. Instead of the escaping rules used by PostgreSQL's standard text format, it produces and recognizes the common CSV escaping mechanism.

> The values in each record are separated by the DELIMITER character. If the value contains the delimiter character, the QUOTE character, the NULL string, a carriage return, or line feed character, then the whole value is prefixed and suffixed by the QUOTE character, and any occurrence within the value of a QUOTE character or the ESCAPE character is preceded by the escape character.

So, it looks like CSV but is using C-style backslash escapes inside values.
Let's remove both backslash and quote from our strings to make PostgreSQL happy.

```
rm dump.csv

SELECT WatchID::Int64, JavaEnable, replaceAll(replaceAll(replaceAll(toValidUTF8(Title), '\0', ''), '"', ''), '\\', ''), GoodEvent, EventTime, EventDate, CounterID::Int32, ClientIP::Int32, RegionID::Int32,
    UserID::Int64, CounterClass, OS, UserAgent, replaceAll(replaceAll(replaceAll(toValidUTF8(URL), '\0', ''), '"', ''), '\\', ''), replaceAll(replaceAll(replaceAll(toValidUTF8(Referer), '\0', ''), '"', ''), '\\', ''), Refresh, RefererCategoryID::Int16, RefererRegionID::Int32,
    URLCategoryID::Int16, URLRegionID::Int32, ResolutionWidth::Int16, ResolutionHeight::Int16, ResolutionDepth, FlashMajor, FlashMinor,
    FlashMinor2, NetMajor, NetMinor, UserAgentMajor::Int16, replaceAll(replaceAll(replaceAll(toValidUTF8(UserAgentMinor::String), '\0', ''), '"', ''), '\\', ''), CookieEnable, JavascriptEnable, IsMobile, MobilePhone,
    replaceAll(replaceAll(replaceAll(toValidUTF8(MobilePhoneModel), '\0', ''), '"', ''), '\\', ''), replaceAll(replaceAll(replaceAll(toValidUTF8(Params), '\0', ''), '"', ''), '\\', ''), IPNetworkID::Int32, TraficSourceID, SearchEngineID::Int16, replaceAll(replaceAll(replaceAll(toValidUTF8(SearchPhrase), '\0', ''), '"', ''), '\\', ''),
    AdvEngineID, IsArtifical, WindowClientWidth::Int16, WindowClientHeight::Int16, ClientTimeZone, ClientEventTime,
    SilverlightVersion1, SilverlightVersion2, SilverlightVersion3::Int32, SilverlightVersion4::Int16, replaceAll(replaceAll(replaceAll(toValidUTF8(PageCharset), '\0', ''), '"', ''), '\\', ''),
    CodeVersion::Int32, IsLink, IsDownload, IsNotBounce, FUniqID::Int64, replaceAll(replaceAll(replaceAll(toValidUTF8(OriginalURL), '\0', ''), '"', ''), '\\', ''), HID::Int32, IsOldCounter, IsEvent,
    IsParameter, DontCountHits, WithHash, replaceAll(replaceAll(replaceAll(toValidUTF8(HitColor::String), '\0', ''), '"', ''), '\\', ''), LocalEventTime, Age, Sex, Income, Interests::Int16, Robotness, RemoteIP::Int32,
    WindowName, OpenerName, HistoryLength, replaceAll(replaceAll(replaceAll(toValidUTF8(BrowserLanguage::String), '\0', ''), '"', ''), '\\', ''), replaceAll(replaceAll(replaceAll(toValidUTF8(BrowserCountry::String), '\0', ''), '"', ''), '\\', ''),
    replaceAll(replaceAll(replaceAll(toValidUTF8(SocialNetwork), '\0', ''), '"', ''), '\\', ''), replaceAll(replaceAll(replaceAll(toValidUTF8(SocialAction), '\0', ''), '"', ''), '\\', ''),
    HTTPError, least(SendTiming, 30000), least(DNSTiming, 30000), least(ConnectTiming, 30000), least(ResponseStartTiming, 30000),
    least(ResponseEndTiming, 30000), least(FetchTiming, 30000), SocialSourceNetworkID,
    replaceAll(replaceAll(replaceAll(toValidUTF8(SocialSourcePage), '\0', ''), '"', ''), '\\', ''), ParamPrice, replaceAll(replaceAll(replaceAll(toValidUTF8(ParamOrderID), '\0', ''), '"', ''), '\\', ''), replaceAll(replaceAll(replaceAll(toValidUTF8(ParamCurrency::String), '\0', ''), '"', ''), '\\', ''),
    ParamCurrencyID::Int16, OpenstatServiceName, OpenstatCampaignID, OpenstatAdID, OpenstatSourceID,
    UTMSource, UTMMedium, UTMCampaign, UTMContent, UTMTerm, FromTag, HasGCLID, RefererHash::Int64, URLHash::Int64, CLID::Int32
FROM hits_100m_obfuscated
INTO OUTFILE 'dump.csv'
FORMAT CSV
```

It does not work at all:

```
$ time sudo -u postgres timescaledb-parallel-copy --db-name tutorial --table hits_100m_obfuscated --file dump.csv --workers 16 --copy-options "CSV" -connection 'host=localhost password=12345'
panic: pq: invalid input syntax for type bigint: "       ПЕСНЮ  ПРЕСТИВАРКЕ ДОЛЖНО ЛИ,1,306,31432,304,22796,1011,879,37,15,5,700.224,2,7,13,D�,1,1,0,0,",",3039109,-1,0,",0,0,779,292,135,2013-07-31 09:37:12,0,0,0,0,windows,1,0,0,0,6888403766694734958,http%3A//maps&sort_order_Kurzarm_DOB&sr=http%3A%2F%3Fpage=/ok.html?1=1&cid=577&oki=1&op_seo_entry=&op_uid=13225;IC"

goroutine 20 [running]:
main.processBatches(0xc0000183d0, 0xc0000a66c0)
        /home/builder/go/src/github.com/timescale/timescaledb-parallel-copy/cmd/timescaledb-parallel-copy/main.go:262 +0x879
created by main.main
        /home/builder/go/src/github.com/timescale/timescaledb-parallel-copy/cmd/timescaledb-parallel-copy/main.go:148 +0x1bb

real    1m47.915s
user    0m33.676s
sys     0m8.028s
```

Maybe let's switch from CSV to TSV that PostgreSQL seems to understand better.

```
SELECT WatchID::Int64, JavaEnable, replaceAll(replaceAll(replaceAll(toValidUTF8(Title), '\0', ''), '"', ''), '\\', ''), GoodEvent, EventTime, EventDate, CounterID::Int32, ClientIP::Int32, RegionID::Int32,
    UserID::Int64, CounterClass, OS, UserAgent, replaceAll(replaceAll(replaceAll(toValidUTF8(URL), '\0', ''), '"', ''), '\\', ''), replaceAll(replaceAll(replaceAll(toValidUTF8(Referer), '\0', ''), '"', ''), '\\', ''), Refresh, RefererCategoryID::Int16, RefererRegionID::Int32,
    URLCategoryID::Int16, URLRegionID::Int32, ResolutionWidth::Int16, ResolutionHeight::Int16, ResolutionDepth, FlashMajor, FlashMinor,
    FlashMinor2, NetMajor, NetMinor, UserAgentMajor::Int16, replaceAll(replaceAll(replaceAll(toValidUTF8(UserAgentMinor::String), '\0', ''), '"', ''), '\\', ''), CookieEnable, JavascriptEnable, IsMobile, MobilePhone,
    replaceAll(replaceAll(replaceAll(toValidUTF8(MobilePhoneModel), '\0', ''), '"', ''), '\\', ''), replaceAll(replaceAll(replaceAll(toValidUTF8(Params), '\0', ''), '"', ''), '\\', ''), IPNetworkID::Int32, TraficSourceID, SearchEngineID::Int16, replaceAll(replaceAll(replaceAll(toValidUTF8(SearchPhrase), '\0', ''), '"', ''), '\\', ''),
    AdvEngineID, IsArtifical, WindowClientWidth::Int16, WindowClientHeight::Int16, ClientTimeZone, ClientEventTime,
    SilverlightVersion1, SilverlightVersion2, SilverlightVersion3::Int32, SilverlightVersion4::Int16, replaceAll(replaceAll(replaceAll(toValidUTF8(PageCharset), '\0', ''), '"', ''), '\\', ''),
    CodeVersion::Int32, IsLink, IsDownload, IsNotBounce, FUniqID::Int64, replaceAll(replaceAll(replaceAll(toValidUTF8(OriginalURL), '\0', ''), '"', ''), '\\', ''), HID::Int32, IsOldCounter, IsEvent,
    IsParameter, DontCountHits, WithHash, replaceAll(replaceAll(replaceAll(toValidUTF8(HitColor::String), '\0', ''), '"', ''), '\\', ''), LocalEventTime, Age, Sex, Income, Interests::Int16, Robotness, RemoteIP::Int32,
    WindowName, OpenerName, HistoryLength, replaceAll(replaceAll(replaceAll(toValidUTF8(BrowserLanguage::String), '\0', ''), '"', ''), '\\', ''), replaceAll(replaceAll(replaceAll(toValidUTF8(BrowserCountry::String), '\0', ''), '"', ''), '\\', ''),
    replaceAll(replaceAll(replaceAll(toValidUTF8(SocialNetwork), '\0', ''), '"', ''), '\\', ''), replaceAll(replaceAll(replaceAll(toValidUTF8(SocialAction), '\0', ''), '"', ''), '\\', ''),
    HTTPError, least(SendTiming, 30000), least(DNSTiming, 30000), least(ConnectTiming, 30000), least(ResponseStartTiming, 30000),
    least(ResponseEndTiming, 30000), least(FetchTiming, 30000), SocialSourceNetworkID,
    replaceAll(replaceAll(replaceAll(toValidUTF8(SocialSourcePage), '\0', ''), '"', ''), '\\', ''), ParamPrice, replaceAll(replaceAll(replaceAll(toValidUTF8(ParamOrderID), '\0', ''), '"', ''), '\\', ''), replaceAll(replaceAll(replaceAll(toValidUTF8(ParamCurrency::String), '\0', ''), '"', ''), '\\', ''),
    ParamCurrencyID::Int16, OpenstatServiceName, OpenstatCampaignID, OpenstatAdID, OpenstatSourceID,
    UTMSource, UTMMedium, UTMCampaign, UTMContent, UTMTerm, FromTag, HasGCLID, RefererHash::Int64, URLHash::Int64, CLID::Int32
FROM hits_100m_obfuscated
INTO OUTFILE 'dump.tsv'
FORMAT TSV
```

But how to pass TSV to `timescaledb-parallel-copy` tool?

```
milovidov@mtlog-perftest03j:~$ time sudo -u postgres timescaledb-parallel-copy --db-name tutorial --table hits_100m_obfuscated --file dump.tsv --workers 16 -connection 'host=localhost password=12345'           panic: pq: invalid input syntax for type bigint: "9076997425961590393\t0\tКино\t1\t2013-07-06 17:47:29\t2013-07-06\t225510\t-1056921538\t229\t3467937489264290637\t0\t2\t3\thttp://liver.ru/belgorod/page/1006.jки/доп_приборы\thttp://video.yandex.ru/1.561.540.000703/?order_Kurzarm_alia\t0\t16124\t20\t14328\t22\t1638\t1658\t23\t15\t7\t700\t0\t0\t17\tD�\t1\t1\t0\t0\t\t\t2095433\t-1\t0\t\t0\t1\t1369\t713\t135\t2013-07-06 16:25:42\t0\t0\t0\t0\twindows\t1601\t0\t0\t0\t5566829288329160346\t\t940752990\t0\t0\t0\t0\t0\t5\t2013-07-06 01:32:13\t55\t2\t3\t0\t2\t-1352932082\t-1\t-1\t-1\tS0\t�\\f\t\t\t0\t0\t0\t0\t0\t0\t0\t0\t\t0\t\tNH\t0\t\t\t\t\t\t\t\t\t\t\t0\t6811023348165660452\t7011450103338277684\t0"

goroutine 20 [running]:
main.processBatches(0xc0000183d0, 0xc0000a66c0)
        /home/builder/go/src/github.com/timescale/timescaledb-parallel-copy/cmd/timescaledb-parallel-copy/main.go:262 +0x879
created by main.main
        /home/builder/go/src/github.com/timescale/timescaledb-parallel-copy/cmd/timescaledb-parallel-copy/main.go:148 +0x1bb

real    0m0.304s
user    0m0.044s
sys     0m0.044s
milovidov@mtlog-perftest03j:~$ time sudo -u postgres timescaledb-parallel-copy --db-name tutorial --table hits_100m_obfuscated --file dump.tsv --copy-options "TEXT" --workers 16 -connection 'host=localhost password=12345'
panic: pq: syntax error at or near "TEXT"

goroutine 18 [running]:
main.processBatches(0xc0000183d0, 0xc0000a66c0)
        /home/builder/go/src/github.com/timescale/timescaledb-parallel-copy/cmd/timescaledb-parallel-copy/main.go:262 +0x879
created by main.main
        /home/builder/go/src/github.com/timescale/timescaledb-parallel-copy/cmd/timescaledb-parallel-copy/main.go:148 +0x1bb

real    0m0.044s
user    0m0.048s
sys     0m0.036s
milovidov@mtlog-perftest03j:~$ time sudo -u postgres timescaledb-parallel-copy --db-name tutorial --table hits_100m_obfuscated --file dump.tsv --copy-options "text" --workers 16 -connection 'host=localhost password=12345'
panic: pq: syntax error at or near "text"

goroutine 18 [running]:
main.processBatches(0xc0000183d0, 0xc0000a66c0)
        /home/builder/go/src/github.com/timescale/timescaledb-parallel-copy/cmd/timescaledb-parallel-copy/main.go:262 +0x879
created by main.main
        /home/builder/go/src/github.com/timescale/timescaledb-parallel-copy/cmd/timescaledb-parallel-copy/main.go:148 +0x1bb
panic: pq: syntax error at or near "text"

goroutine 19 [running]:
main.processBatches(0xc0000183d0, 0xc0000a66c0)
        /home/builder/go/src/github.com/timescale/timescaledb-parallel-copy/cmd/timescaledb-parallel-copy/main.go:262 +0x879
created by main.main
        /home/builder/go/src/github.com/timescale/timescaledb-parallel-copy/cmd/timescaledb-parallel-copy/main.go:148 +0x1bb

real    0m0.057s
user    0m0.060s
sys     0m0.028s
milovidov@mtlog-perftest03j:~$ time sudo -u postgres timescaledb-parallel-copy --db-name tutorial --table hits_100m_obfuscated --file dump.tsv --copy-options "Text" --workers 16 -connection 'host=localhost password=12345'
panic: pq: syntax error at or near "Text"

goroutine 11 [running]:
main.processBatches(0xc0000183d0, 0xc0000a66c0)
        /home/builder/go/src/github.com/timescale/timescaledb-parallel-copy/cmd/timescaledb-parallel-copy/main.go:262 +0x879
created by main.main
        /home/builder/go/src/github.com/timescale/timescaledb-parallel-copy/cmd/timescaledb-parallel-copy/main.go:148 +0x1bb

real    0m0.041s
user    0m0.052s
sys     0m0.032s
milovidov@mtlog-perftest03j:~$ time sudo -u postgres timescaledb-parallel-copy --db-name tutorial --table hits_100m_obfuscated --file dump.tsv --copy-options "FORMAT text" --workers 16 -connection 'host=localhost password=12345'
panic: pq: syntax error at or near "FORMAT"

goroutine 21 [running]:
main.processBatches(0xc00019a350, 0xc00019e660)
        /home/builder/go/src/github.com/timescale/timescaledb-parallel-copy/cmd/timescaledb-parallel-copy/main.go:262 +0x879
created by main.main
        /home/builder/go/src/github.com/timescale/timescaledb-parallel-copy/cmd/timescaledb-parallel-copy/main.go:148 +0x1bb

real    0m0.045s
user    0m0.052s
sys     0m0.028s
```

Nothing works:

```
milovidov@mtlog-perftest03j:~$ time sudo -u postgres timescaledb-parallel-copy --help
Usage of timescaledb-parallel-copy:
  -batch-size int
        Number of rows per insert (default 5000)
  -columns string
        Comma-separated columns present in CSV
  -connection string
        PostgreSQL connection url (default "host=localhost user=postgres sslmode=disable")
  -copy-options string
        Additional options to pass to COPY (e.g., NULL 'NULL') (default "CSV")
  -db-name string
        Database where the destination table exists
  -file string
        File to read from rather than stdin
  -header-line-count int
        Number of header lines (default 1)
  -limit int
        Number of rows to insert overall; 0 means to insert all
  -log-batches
        Whether to time individual batches.
  -reporting-period duration
        Period to report insert stats; if 0s, intermediate results will not be reported
  -schema string
        Destination table's schema (default "public")
  -skip-header
        Skip the first line of the input
  -split string
        Character to split by (default ",")
  -table string
        Destination table for insertions (default "test_table")
  -token-size int
        Maximum size to use for tokens. By default, this is 64KB, so any value less than that will be ignored (default 65536)
  -truncate
        Truncate the destination table before insert
  -verbose
        Print more information about copying statistics
  -version
        Show the version of this tool
  -workers int
        Number of parallel requests to make (default 1)

real    0m0.009s
user    0m0.004s
sys     0m0.000s
milovidov@mtlog-perftest03j:~$ time sudo -u postgres timescaledb-parallel-copy --db-name tutorial --table hits_100m_obfuscated --file dump.tsv --truncate --copy-options "" --workers 16 -connection 'host=localhost password=12345'
panic: pq: invalid input syntax for type bigint: "9076997425961590393   0       Кино    1       2013-07-06 17:47:29     2013-07-06      225510  -1056921538     229     3467937489264290637     0       2       3http://liver.ru/belgorod/page/1006.jки/доп_приборы       http://video.yandex.ru/1.561.540.000703/?order_Kurzarm_alia     0       16124   20      14328   22      1638    1658    23      15      7       700     0017      D�      1       1       0       0                       2095433 -1      0               0       1       1369    713     135     2013-07-06 16:25:42     0       0       0       0       windows 1601    000       5566829288329160346             940752990       0       0       0       0       0       5       2013-07-06 01:32:13     55      2       3       0       2       -1352932082     -1      -1      -1      S0�\f                     0       0       0       0       0       0       0       0               0               NH      0                                                                                       06811023348165660452      7011450103338277684     0"

goroutine 13 [running]:
main.processBatches(0xc000019140, 0xc0001eb080)
        /home/builder/go/src/github.com/timescale/timescaledb-parallel-copy/cmd/timescaledb-parallel-copy/main.go:262 +0x879
created by main.main
        /home/builder/go/src/github.com/timescale/timescaledb-parallel-copy/cmd/timescaledb-parallel-copy/main.go:148 +0x1bb

real    0m0.191s
user    0m0.036s
sys     0m0.040s
milovidov@mtlog-perftest03j:~$ time sudo -u postgres timescaledb-parallel-copy --db-name tutorial --table hits_100m_obfuscated --file dump.tsv --truncate --copy-options "NULL AS '\N'" --workers 16 -connection 'host=localhost password=12345'
panic: pq: invalid input syntax for type bigint: "9076997425961590393   0       Кино    1       2013-07-06 17:47:29     2013-07-06      225510  -1056921538     229     3467937489264290637     0       2       3http://liver.ru/belgorod/page/1006.jки/доп_приборы       http://video.yandex.ru/1.561.540.000703/?order_Kurzarm_alia     0       16124   20      14328   22      1638    1658    23      15      7       700     0017      D�      1       1       0       0                       2095433 -1      0               0       1       1369    713     135     2013-07-06 16:25:42     0       0       0       0       windows 1601    000       5566829288329160346             940752990       0       0       0       0       0       5       2013-07-06 01:32:13     55      2       3       0       2       -1352932082     -1      -1      -1      S0�\f                     0       0       0       0       0       0       0       0               0               NH      0                                                                                       06811023348165660452      7011450103338277684     0"

goroutine 11 [running]:
main.processBatches(0xc000018900, 0xc0002886c0)
        /home/builder/go/src/github.com/timescale/timescaledb-parallel-copy/cmd/timescaledb-parallel-copy/main.go:262 +0x879
created by main.main
        /home/builder/go/src/github.com/timescale/timescaledb-parallel-copy/cmd/timescaledb-parallel-copy/main.go:148 +0x1bb

real    0m0.187s
user    0m0.020s
sys     0m0.048s
milovidov@mtlog-perftest03j:~$ time sudo -u postgres timescaledb-parallel-copy --db-name tutorial --table hits_100m_obfuscated --file dump.tsv --truncate --copy-options "DELIMITER AS '\t'" --workers 16 -connection 'host=localhost password=12345'
panic: pq: conflicting or redundant options

goroutine 13 [running]:
main.processBatches(0xc000019140, 0xc0001e9080)
        /home/builder/go/src/github.com/timescale/timescaledb-parallel-copy/cmd/timescaledb-parallel-copy/main.go:262 +0x879
created by main.main
        /home/builder/go/src/github.com/timescale/timescaledb-parallel-copy/cmd/timescaledb-parallel-copy/main.go:148 +0x1bb

real    0m0.196s
user    0m0.048s
sys     0m0.020s
milovidov@mtlog-perftest03j:~$ time sudo -u postgres timescaledb-parallel-copy --db-name tutorial --table hits_100m_obfuscated --file dump.tsv --truncate --copy-options "TEXT DELIMITER AS '\t'" --workers 16 -connection 'host=localhost password=12345'
panic: pq: syntax error at or near "TEXT"

goroutine 22 [running]:
main.processBatches(0xc000019140, 0xc0001e9080)
        /home/builder/go/src/github.com/timescale/timescaledb-parallel-copy/cmd/timescaledb-parallel-copy/main.go:262 +0x879
created by main.main
        /home/builder/go/src/github.com/timescale/timescaledb-parallel-copy/cmd/timescaledb-parallel-copy/main.go:148 +0x1bb
panic: pq: syntax error at or near "TEXT"

goroutine 11 [running]:
main.processBatches(0xc000019140, 0xc0001e9080)
        /home/builder/go/src/github.com/timescale/timescaledb-parallel-copy/cmd/timescaledb-parallel-copy/main.go:262 +0x879
created by main.main
        /home/builder/go/src/github.com/timescale/timescaledb-parallel-copy/cmd/timescaledb-parallel-copy/main.go:148 +0x1bb

real    0m0.191s
user    0m0.032s
sys     0m0.036s
milovidov@mtlog-perftest03j:~$ time sudo -u postgres timescaledb-parallel-copy --db-name tutorial --table hits_100m_obfuscated --file dump.tsv --truncate --copy-options "DELIMITER AS e'\t'" --workers 16 -connection 'host=localhost password=12345'
panic: pq: conflicting or redundant options

goroutine 26 [running]:
main.processBatches(0xc0001330d0, 0xc0001e3020)
        /home/builder/go/src/github.com/timescale/timescaledb-parallel-copy/cmd/timescaledb-parallel-copy/main.go:262 +0x879
created by main.main
        /home/builder/go/src/github.com/timescale/timescaledb-parallel-copy/cmd/timescaledb-parallel-copy/main.go:148 +0x1bb

real    0m0.169s
user    0m0.056s
sys     0m0.016s
```

I will try to avoid `timescaledb-parallel-copy` and use `psql` instead.

```
milovidov@mtlog-perftest03j:~$ sudo -u postgres psql
psql (13.4 (Ubuntu 13.4-4.pgdg18.04+1))
Type "help" for help.

postgres=# \c tutorial
You are now connected to database "tutorial" as user "postgres".
tutorial=# timing
tutorial-# COPY hits_100m_obfuscated FROM 'dump.tsv'
tutorial-# ;
ERROR:  syntax error at or near "timing"
LINE 1: timing
        ^
tutorial=# \timing
Timing is on.
tutorial=# COPY hits_100m_obfuscated FROM 'dump.tsv';
ERROR:  could not open file "dump.tsv" for reading: No such file or directory
HINT:  COPY FROM instructs the PostgreSQL server process to read a file. You may want a client-side facility such as psql's \copy.
Time: 4.348 ms
tutorial=# \copy hits_100m_obfuscated FROM 'dump.tsv';
```

It started to do something... fairly slow with using less than one CPU core.

Folks from TimescaleDB always recommend to enable compression, which is not by default.
Let's read about it:

https://docs.timescale.com/timescaledb/latest/how-to-guides/compression/

> We strongly recommend that you understand how compression works before you start enabling it on your hypertables.

The amount of hackery to overcome PostgreSQL limitations is overwhelming:

> When compression is enabled, TimescaleDB converts data stored in many rows into an array. This means that instead of using lots of rows to store the data, it stores the same data in a single row.

In the meantime, copy finished in "just" 1.5 hours, 19 245 rows/second. This is extremely slow, even for single core.

```
tutorial=# \copy hits_100m_obfuscated FROM 'dump.tsv';
COPY 100000000
Time: 5195909.154 ms (01:26:35.909)
```

## Running Benchmark

Let's prepare for benchmark...
What is needed to execute single query in batch mode?

`man psql`

```
sudo -u postgres psql tutorial -t -c '\timing' -c 'SELECT 1' | grep 'Time'
```

Now we are ready to run our benchmark.

PostgreSQL does not have `SHOW PROCESSLIST`.
It has `select * from pg_stat_activity;` instead.

https://ma.ttias.be/show-full-processlist-equivalent-of-mysql-for-postgresql/

But it does not show query progress.
The first query `SELECT count(*) FROM hits_100m_obfuscated` just hanged. It reads something from disk...

Let's check the data volume:

```
$ sudo du -hcs /opt/postgresql/
68G     /opt/postgresql/
```

Looks consistent for uncompressed data.

```
./benchmark.sh

grep -oP 'Time: \d+' log | grep -oP '\d+' | awk '{ if (n % 3 == 0) { printf("[") }; ++n; printf("%g", $1 / 1000); if (n % 3 == 0) { printf("],\n") } else { printf(", ") } }'
```

Now let's enable compression.

```
ALTER TABLE hits_100m_obfuscated SET (timescaledb.compress);
SELECT add_compression_policy('hits_100m_obfuscated', INTERVAL '0 seconds');
```

```
milovidov@mtlog-perftest03j:~ClickHouse/benchmark/timescaledb$ sudo -u postgres psql tutorial
psql (13.4 (Ubuntu 13.4-4.pgdg18.04+1))
Type "help" for help.

tutorial=# ALTER TABLE hits_100m_obfuscated SET (timescaledb.compress);
ALTER TABLE
tutorial=# SELECT add_compression_policy('hits_100m_obfuscated', INTERVAL '0 seconds');
 add_compression_policy
------------------------
                   1000
(1 row)
```

Ok, in `top` I see that it started compression with using single CPU core.

```
300464 postgres  20   0 32.456g 932044 911452 D  48.0  0.7   1:08.11 postgres: 13/main: Compression Policy [1000]
```

Let's also define better order of data:

```
ALTER TABLE hits_100m_obfuscated
  SET (timescaledb.compress,
       timescaledb.compress_orderby = 'counterid, userid, event_time');
```

The query hanged. Maybe it's waiting for finish of previous compression?

After several minutes it answered:

```
ERROR:  cannot change configuration on already compressed chunks
DETAIL:  There are compressed chunks that prevent changing the existing compression configuration.
```

Ok, at least some of the chunks will have the proper order.

After a few hours looks like the compression finished.

```
sudo ncdu /var/lib/postgresql/13/main/

28.9 GiB [##########] /base
```

Yes, looks like it's compressed. About two times - not too much.

Let's rerun the benchmark.

Ok, it's slightly faster.
