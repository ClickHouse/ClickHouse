Export data from ClickHouse:

```
SELECT * FROM hits_100m_obfuscated INTO OUTFILE 'hits.parquet' FORMAT Parquet

$ wc -c hits.parquet
17193559098 hits.parquet
```

Install DuckDB:

```
wget https://github.com/duckdb/duckdb/releases/download/v0.3.0/duckdb_cli-linux-amd64.zip
unzip duckdb_cli-linux-amd64.zip
```

Try DuckDB:

```
milovidov@mtlog-perftest03j:~$ ./duckdb
v0.3.0 46a0fc50a
Enter ".help" for usage hints.
Connected to a transient in-memory database.
Use ".open FILENAME" to reopen on a persistent database.
D ?
>
> ;
Error: Parser Error: syntax error at or near "?"
LINE 1: ?
        ^
D help;
Error: Parser Error: syntax error at or near "help"
LINE 1: help;
        ^
D h;
Error: Parser Error: syntax error at or near "h"
LINE 1: h;
        ^
D .?
Error: unknown command or invalid arguments:  "?". Enter ".help" for help
D .help
.auth ON|OFF             Show authorizer callbacks
.backup ?DB? FILE        Backup DB (default "main") to FILE
.bail on|off             Stop after hitting an error.  Default OFF
.binary on|off           Turn binary output on or off.  Default OFF
.cd DIRECTORY            Change the working directory to DIRECTORY
.changes on|off          Show number of rows changed by SQL
.check GLOB              Fail if output since .testcase does not match
.clone NEWDB             Clone data into NEWDB from the existing database
.databases               List names and files of attached databases
.dbconfig ?op? ?val?     List or change sqlite3_db_config() options
.dbinfo ?DB?             Show status information about the database
.dump ?TABLE?            Render database content as SQL
.echo on|off             Turn command echo on or off
.eqp on|off|full|...     Enable or disable automatic EXPLAIN QUERY PLAN
.excel                   Display the output of next command in spreadsheet
.exit ?CODE?             Exit this program with return-code CODE
.expert                  EXPERIMENTAL. Suggest indexes for queries
.explain ?on|off|auto?   Change the EXPLAIN formatting mode.  Default: auto
.filectrl CMD ...        Run various sqlite3_file_control() operations
.fullschema ?--indent?   Show schema and the content of sqlite_stat tables
.headers on|off          Turn display of headers on or off
.help ?-all? ?PATTERN?   Show help text for PATTERN
.import FILE TABLE       Import data from FILE into TABLE
.imposter INDEX TABLE    Create imposter table TABLE on index INDEX
.indexes ?TABLE?         Show names of indexes
.limit ?LIMIT? ?VAL?     Display or change the value of an SQLITE_LIMIT
.lint OPTIONS            Report potential schema issues.
.log FILE|off            Turn logging on or off.  FILE can be stderr/stdout
.mode MODE ?TABLE?       Set output mode
.nullvalue STRING        Use STRING in place of NULL values
.once ?OPTIONS? ?FILE?   Output for the next SQL command only to FILE
.open ?OPTIONS? ?FILE?   Close existing database and reopen FILE
.output ?FILE?           Send output to FILE or stdout if FILE is omitted
.parameter CMD ...       Manage SQL parameter bindings
.print STRING...         Print literal STRING
.progress N              Invoke progress handler after every N opcodes
.prompt MAIN CONTINUE    Replace the standard prompts
.quit                    Exit this program
.read FILE               Read input from FILE
.restore ?DB? FILE       Restore content of DB (default "main") from FILE
.save FILE               Write in-memory database into FILE
.scanstats on|off        Turn sqlite3_stmt_scanstatus() metrics on or off
.schema ?PATTERN?        Show the CREATE statements matching PATTERN
.selftest ?OPTIONS?      Run tests defined in the SELFTEST table
.separator COL ?ROW?     Change the column and row separators
.sha3sum ...             Compute a SHA3 hash of database content
.shell CMD ARGS...       Run CMD ARGS... in a system shell
.show                    Show the current values for various settings
.stats ?on|off?          Show stats or turn stats on or off
.system CMD ARGS...      Run CMD ARGS... in a system shell
.tables ?TABLE?          List names of tables matching LIKE pattern TABLE
.testcase NAME           Begin redirecting output to 'testcase-out.txt'
.testctrl CMD ...        Run various sqlite3_test_control() operations
.timeout MS              Try opening locked tables for MS milliseconds
.timer on|off            Turn SQL timer on or off
.trace ?OPTIONS?         Output each SQL statement as it is run
.vfsinfo ?AUX?           Information about the top-level VFS
.vfslist                 List all available VFSes
.vfsname ?AUX?           Print the name of the VFS stack
.width NUM1 NUM2 ...     Set minimum column widths for columnar output
D q
> .q
> ;
Error: Parser Error: syntax error at or near "q"
LINE 1: q
        ^
D .q
```

Let's load the data:

```
D CREATE TABLE hits AS SELECT * FROM parquet_scan('hits.parquet')
> ;
```

It is using single CPU core and accumulating data in memory.

```
Killed
```

As expected. My server has "only" 128 GiB RAM.

Let's free some memory and run again:

```
Error: Out of Memory Error: could not allocate block of 262144 bytes
Database is launched in in-memory mode and no temporary directory is specified.
Unused blocks cannot be offloaded to disk.

Launch the database with a persistent storage back-end
Or set PRAGMA temp_directory='/path/to/tmp.tmp'
```

Now it works:

```
D PRAGMA temp_directory='duckdb.tmp'
> ;
D CREATE TABLE hits AS SELECT * FROM parquet_scan('hits.parquet');
D SELECT count(*) FROM hits;
┌──────────────┐
│ count_star() │
├──────────────┤
│ 100000000    │
└──────────────┘
D SELECT AdvEngineID, count(*) FROM hits WHERE AdvEngineID != 0 GROUP BY AdvEngineID ORDER BY count(*) DESC;
┌─────────────┬──────────────┐
│ AdvEngineID │ count_star() │
├─────────────┼──────────────┤
│ 2           │ 404620       │
│ 27          │ 113167       │
│ 13          │ 45633        │
│ 45          │ 38974        │
│ 44          │ 9731         │
│ 3           │ 6896         │
│ 62          │ 5266         │
│ 52          │ 3554         │
│ 50          │ 938          │
│ 28          │ 836          │
│ 53          │ 350          │
│ 25          │ 343          │
│ 61          │ 158          │
│ 21          │ 38           │
│ 42          │ 20           │
│ 16          │ 7            │
│ 7           │ 3            │
│ 22          │ 1            │
└─────────────┴──────────────┘
```

But saving the database does not work:

```
D .save 'duckdb.data'
sqlite3_backup_init: unsupported.
Error:
```

Let's simply paste queries into CLI.

```
D SELECT count(*) FROM hits WHERE AdvEngineID != 0;
┌──────────────┐
│ count_star() │
├──────────────┤
│ 630535       │
└──────────────┘
Run Time: real 0.227 user 0.228000 sys 0.000000
D SELECT sum(AdvEngineID), count(*), avg(ResolutionWidth) FROM hits;
┌──────────────────┬──────────────┬──────────────────────┐
│ sum(advengineid) │ count_star() │ avg(resolutionwidth) │
├──────────────────┼──────────────┼──────────────────────┤
│ 7280824          │ 100000000    │ 1513.48908394        │
└──────────────────┴──────────────┴──────────────────────┘
Run Time: real 0.678 user 0.508000 sys 0.008000
D SELECT sum(UserID) FROM hits;
┌────────────────────────┐
│      sum(userid)       │
├────────────────────────┤
│ 3.2306058693988996e+26 │
└────────────────────────┘
Run Time: real 0.697 user 0.448000 sys 0.020000
D SELECT COUNT(DISTINCT UserID) FROM hits;
┌───────────────┐
│ count(userid) │
├───────────────┤
│ 17630976      │
└───────────────┘
Run Time: real 7.928 user 7.164000 sys 0.660000
D SELECT COUNT(DISTINCT SearchPhrase) FROM hits;u
┌─────────────────────┐
│ count(searchphrase) │
├─────────────────────┤
│ 6019589             │
└─────────────────────┘
Run Time: real 12.403 user 10.820000 sys 0.208000
D SELECT min(EventDate), max(EventDate) FROM hits;
R┌────────────────┬────────────────┐
│ min(eventdate) │ max(eventdate) │
├────────────────┼────────────────┤
│ 15888          │ 15917          │
└────────────────┴────────────────┘
Run Time: real 0.604 user 0.376000 sys 0.008000
D SELECT AdvEngineID, count(*) FROM hits WHERE AdvEngineID != 0 GROUP BY AdvEngineID ORDER BY count(*) DESC;(
┌─────────────┬──────────────┐
│ AdvEngineID │ count_star() │
├─────────────┼──────────────┤
│ 2           │ 404620       │
│ 27          │ 113167       │
│ 13          │ 45633        │
│ 45          │ 38974        │
│ 44          │ 9731         │
│ 3           │ 6896         │
│ 62          │ 5266         │
│ 52          │ 3554         │
│ 50          │ 938          │
│ 28          │ 836          │
│ 53          │ 350          │
│ 25          │ 343          │
│ 61          │ 158          │
│ 21          │ 38           │
│ 42          │ 20           │
│ 16          │ 7            │
│ 7           │ 3            │
│ 22          │ 1            │
└─────────────┴──────────────┘
Run Time: real 0.344 user 0.344000 sys 0.000000
D SELECT RegionID, COUNT(DISTINCT UserID) AS u FROM hits GROUP BY RegionID ORDER BY u DESC LIMIT 10;
┌──────────┬─────────┐
│ RegionID │    u    │
├──────────┼─────────┤
│ 229      │ 2845673 │
│ 2        │ 1081016 │
│ 208      │ 831676  │
│ 169      │ 604583  │
│ 184      │ 322661  │
│ 158      │ 307152  │
│ 34       │ 299479  │
│ 55       │ 286525  │
│ 107      │ 272448  │
│ 42       │ 243181  │
└──────────┴─────────┘
Run Time: real 8.872 user 7.592000 sys 0.108000
D SELECT RegionID, sum(AdvEngineID), count(*) AS c, avg(ResolutionWidth), COUNT(DISTINCT UserID) FROM hits GROUP BY RegionID ORDER BY c DESC LIMIT 10;l
┌──────────┬──────────────────┬──────────┬──────────────────────┬───────────────┐
│ RegionID │ sum(advengineid) │    c     │ avg(resolutionwidth) │ count(userid) │
├──────────┼──────────────────┼──────────┼──────────────────────┼───────────────┤
│ 229      │ 2078084          │ 18296430 │ 1506.0876750819696   │ 2845673       │
│ 2        │ 441711           │ 6687708  │ 1479.8410618406187   │ 1081016       │
│ 208      │ 285925           │ 4261945  │ 1285.260504769536    │ 831676        │
│ 169      │ 100887           │ 3320286  │ 1465.90517142198     │ 604583        │
│ 32       │ 81498            │ 1843721  │ 1538.0370495318978   │ 216010        │
│ 34       │ 161779           │ 1792406  │ 1548.364990409539    │ 299479        │
│ 184      │ 55526            │ 1755223  │ 1506.8102679830426   │ 322661        │
│ 42       │ 108820           │ 1542771  │ 1587.1074287758845   │ 243181        │
│ 107      │ 120470           │ 1516722  │ 1548.6039623609336   │ 272448        │
│ 51       │ 98212            │ 1435598  │ 1579.8864215469791   │ 211505        │
└──────────┴──────────────────┴──────────┴──────────────────────┴───────────────┘
Run Time: real 8.447 user 8.444000 sys 0.000000
D SELECT MobilePhoneModel, COUNT(DISTINCT UserID) AS u FROM hits WHERE MobilePhoneModel != '' GROUP BY MobilePhoneModel ORDER BY u DESC LIMIT 10;
t┌──────────────────┬─────────┐
│ MobilePhoneModel │    u    │
├──────────────────┼─────────┤
│ iPad             │ 1090347 │
│ iPhone           │ 45758   │
│ A500             │ 16046   │
│ N8-00            │ 5565    │
│ iPho             │ 3300    │
│ ONE TOUCH 6030A  │ 2759    │
│ GT-P7300B        │ 1907    │
│ 3110000          │ 1871    │
│ GT-I9500         │ 1598    │
│ eagle75          │ 1492    │
└──────────────────┴─────────┘
Run Time: real 5.077 user 4.416000 sys 0.032000
D SELECT MobilePhone, MobilePhoneModel, COUNT(DISTINCT UserID) AS u FROM hits WHERE MobilePhoneModel != '' GROUP BY MobilePhone, MobilePhoneModel ORDER BY u DESC LIMIT 10;,
┌─────────────┬──────────────────┬────────┐
│ MobilePhone │ MobilePhoneModel │   u    │
├─────────────┼──────────────────┼────────┤
│ 1           │ iPad             │ 931038 │
│ 5           │ iPad             │ 48385  │
│ 6           │ iPad             │ 29710  │
│ 7           │ iPad             │ 28391  │
│ 118         │ A500             │ 16005  │
│ 6           │ iPhone           │ 14516  │
│ 26          │ iPhone           │ 13566  │
│ 10          │ iPad             │ 11433  │
│ 32          │ iPad             │ 9503   │
│ 13          │ iPad             │ 9417   │
└─────────────┴──────────────────┴────────┘
Run Time: real 5.193 user 4.916000 sys 0.012000
D SELECT SearchPhrase, count(*) AS c FROM hits WHERE SearchPhrase != '' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10;
l┌────────────────────────────────────────────────────────────────────────────────────┬───────┐
│                                    SearchPhrase                                    │   c   │
├────────────────────────────────────────────────────────────────────────────────────┼───────┤
│ \xD0\xBA\xD0\xB0\xD1\x80\xD0\xB5\xD0\xBB\xD0\xBA\xD0\xB8                           │ 70263 │
│ \xD0\xB0\xD0\xBB\xD0\xB1\xD0\xB0\xD1\x82\xD1\x80\xD1\x83\xD1\x82\xD0\xB4\xD0\xB... │ 34675 │
│ \xD1\x81\xD0\xBC\xD0\xBE\xD1\x82\xD1\x80\xD0\xB5\xD1\x82\xD1\x8C \xD0\xBE\xD0\x... │ 24579 │
│ \xD1\x81\xD0\xBC\xD0\xBE\xD1\x82\xD1\x80\xD0\xB5\xD1\x82\xD1\x8C \xD0\xBE\xD0\x... │ 21649 │
│ \xD1\x81\xD0\xBC\xD0\xBE\xD1\x82\xD1\x80\xD0\xB5\xD1\x82\xD1\x8C                   │ 19703 │
│ \xD0\xBC\xD0\xB0\xD0\xBD\xD0\xB3\xD1\x83 \xD0\xB2 \xD0\xB7\xD0\xB0\xD1\x80\xD0\... │ 19195 │
│ \xD0\xB4\xD1\x80\xD1\x83\xD0\xB6\xD0\xBA\xD0\xB5 \xD0\xBF\xD0\xBE\xD0\xBC\xD0\x... │ 17284 │
│ galaxy table                                                                       │ 16746 │
│ \xD1\x8D\xD0\xBA\xD0\xB7\xD0\xBE\xD0\xB8\xD0\xB4\xD0\xBD\xD1\x8B\xD0\xB5           │ 16620 │
│ \xD1\x81\xD0\xBA\xD0\xBE\xD0\xBB\xD1\x8C\xD0\xBA\xD0\xBE \xD0\xBC\xD1\x8B\xD1\x... │ 12317 │
└────────────────────────────────────────────────────────────────────────────────────┴───────┘
Run Time: real 8.085 user 8.040000 sys 0.044000
D SELECT SearchPhrase, COUNT(DISTINCT UserID) AS u FROM hits WHERE SearchPhrase != '' GROUP BY SearchPhrase ORDER BY u DESC LIMIT 10;e
┌────────────────────────────────────────────────────────────────────────────────────┬───────┐
│                                    SearchPhrase                                    │   u   │
├────────────────────────────────────────────────────────────────────────────────────┼───────┤
│ \xD0\xBA\xD0\xB0\xD1\x80\xD0\xB5\xD0\xBB\xD0\xBA\xD0\xB8                           │ 23673 │
│ \xD1\x81\xD0\xBC\xD0\xBE\xD1\x82\xD1\x80\xD0\xB5\xD1\x82\xD1\x8C \xD0\xBE\xD0\x... │ 19743 │
│ \xD0\xB0\xD0\xBB\xD0\xB1\xD0\xB0\xD1\x82\xD1\x80\xD1\x83\xD1\x82\xD0\xB4\xD0\xB... │ 18394 │
│ \xD1\x81\xD0\xBC\xD0\xBE\xD1\x82\xD1\x80\xD0\xB5\xD1\x82\xD1\x8C \xD0\xBE\xD0\x... │ 17553 │
│ \xD1\x81\xD0\xBC\xD0\xBE\xD1\x82\xD1\x80\xD0\xB5\xD1\x82\xD1\x8C                   │ 14600 │
│ \xD1\x8D\xD0\xBA\xD0\xB7\xD0\xBE\xD0\xB8\xD0\xB4\xD0\xBD\xD1\x8B\xD0\xB5           │ 14529 │
│ \xD0\xBC\xD0\xB0\xD0\xBD\xD0\xB3\xD1\x83 \xD0\xB2 \xD0\xB7\xD0\xB0\xD1\x80\xD0\... │ 14198 │
│ \xD1\x81\xD0\xBA\xD0\xBE\xD0\xBB\xD1\x8C\xD0\xBA\xD0\xBE \xD0\xBC\xD1\x8B\xD1\x... │ 9007  │
│ \xD0\xB4\xD1\x80\xD1\x83\xD0\xB6\xD0\xBA\xD0\xB5 \xD0\xBF\xD0\xBE\xD0\xBC\xD0\x... │ 8792  │
│ \xD0\xBA\xD0\xBE\xD0\xBC\xD0\xB1\xD0\xB8\xD0\xBD\xD0\xB8\xD1\x80\xD0\xBE\xD0\xB... │ 7572  │
└────────────────────────────────────────────────────────────────────────────────────┴───────┘
Run Time: real 14.516 user 12.960000 sys 1.196000
D SELECT SearchEngineID, SearchPhrase, count(*) AS c FROM hits WHERE SearchPhrase != '' GROUP BY SearchEngineID, SearchPhrase ORDER BY c DESC LIMIT 10;
r┌────────────────┬────────────────────────────────────────────────────────────────────────────────────┬───────┐
│ SearchEngineID │                                    SearchPhrase                                    │   c   │
├────────────────┼────────────────────────────────────────────────────────────────────────────────────┼───────┤
│ 2              │ \xD0\xBA\xD0\xB0\xD1\x80\xD0\xB5\xD0\xBB\xD0\xBA\xD0\xB8                           │ 46258 │
│ 2              │ \xD0\xBC\xD0\xB0\xD0\xBD\xD0\xB3\xD1\x83 \xD0\xB2 \xD0\xB7\xD0\xB0\xD1\x80\xD0\... │ 18871 │
│ 2              │ \xD1\x81\xD0\xBC\xD0\xBE\xD1\x82\xD1\x80\xD0\xB5\xD1\x82\xD1\x8C \xD0\xBE\xD0\x... │ 16905 │
│ 3              │ \xD0\xB0\xD0\xBB\xD0\xB1\xD0\xB0\xD1\x82\xD1\x80\xD1\x83\xD1\x82\xD0\xB4\xD0\xB... │ 16748 │
│ 2              │ \xD1\x81\xD0\xBC\xD0\xBE\xD1\x82\xD1\x80\xD0\xB5\xD1\x82\xD1\x8C \xD0\xBE\xD0\x... │ 14911 │
│ 2              │ \xD0\xB0\xD0\xBB\xD0\xB1\xD0\xB0\xD1\x82\xD1\x80\xD1\x83\xD1\x82\xD0\xB4\xD0\xB... │ 13716 │
│ 2              │ \xD1\x8D\xD0\xBA\xD0\xB7\xD0\xBE\xD0\xB8\xD0\xB4\xD0\xBD\xD1\x8B\xD0\xB5           │ 13414 │
│ 2              │ \xD1\x81\xD0\xBC\xD0\xBE\xD1\x82\xD1\x80\xD0\xB5\xD1\x82\xD1\x8C                   │ 13105 │
│ 3              │ \xD0\xBA\xD0\xB0\xD1\x80\xD0\xB5\xD0\xBB\xD0\xBA\xD0\xB8                           │ 12815 │
│ 2              │ \xD0\xB4\xD1\x80\xD1\x83\xD0\xB6\xD0\xBA\xD0\xB5 \xD0\xBF\xD0\xBE\xD0\xBC\xD0\x... │ 11946 │
└────────────────┴────────────────────────────────────────────────────────────────────────────────────┴───────┘
Run Time: real 8.029 user 7.544000 sys 0.016000
D SELECT UserID, count(*) FROM hits GROUP BY UserID ORDER BY count(*) DESC LIMIT 10;s
┌─────────────────────┬──────────────┐
│       UserID        │ count_star() │
├─────────────────────┼──────────────┤
│ 1313338681122956954 │ 29097        │
│ 1907779576417363396 │ 25333        │
│ 2305303682471783379 │ 10611        │
│ 7982623143712728547 │ 7584         │
│ 6018350421959114808 │ 6678         │
│ 7280399273658728997 │ 6411         │
│ 1090981537032625727 │ 6197         │
│ 5730251990344211405 │ 6019         │
│ 835157184735512989  │ 5211         │
│ 770542365400669095  │ 4906         │
└─────────────────────┴──────────────┘
Run Time: real 5.225 user 5.224000 sys 0.000000
D SELECT UserID, SearchPhrase, count(*) FROM hits GROUP BY UserID, SearchPhrase ORDER BY count(*) DESC LIMIT 10;,
┌─────────────────────┬──────────────┬──────────────┐
│       UserID        │ SearchPhrase │ count_star() │
├─────────────────────┼──────────────┼──────────────┤
│ 1313338681122956954 │              │ 29097        │
│ 1907779576417363396 │              │ 25333        │
│ 2305303682471783379 │              │ 10611        │
│ 7982623143712728547 │              │ 6669         │
│ 7280399273658728997 │              │ 6408         │
│ 1090981537032625727 │              │ 6196         │
│ 5730251990344211405 │              │ 6019         │
│ 6018350421959114808 │              │ 5990         │
│ 835157184735512989  │              │ 5209         │
│ 770542365400669095  │              │ 4906         │
└─────────────────────┴──────────────┴──────────────┘
Run Time: real 14.506 user 13.748000 sys 0.496000
D SELECT UserID, SearchPhrase, count(*) FROM hits GROUP BY UserID, SearchPhrase LIMIT 10;
┌──────────────────────┬────────────────────────────────────────────────────────────────────────────────────┬──────────────┐
│        UserID        │                                    SearchPhrase                                    │ count_star() │
├──────────────────────┼────────────────────────────────────────────────────────────────────────────────────┼──────────────┤
│ 427738049800818189   │                                                                                    │ 1            │
│ 15985305027620249815 │                                                                                    │ 6            │
│ 7418527520126366595  │                                                                                    │ 1            │
│ 519640690937130534   │                                                                                    │ 2            │
│ 376160620089546609   │                                                                                    │ 1            │
│ 4523925649124320482  │                                                                                    │ 1            │
│ 2523324276554785406  │                                                                                    │ 2            │
│ 6025915247311731176  │                                                                                    │ 26           │
│ 6329532664518159520  │ \xD0\xB2\xD0\xB5\xD0\xB4\xD0\xBE\xD0\xBC\xD0\xBE\xD1\x81\xD0\xBA\xD0\xB2\xD1\x8... │ 2            │
│ 6329532664518159520  │                                                                                    │ 19           │
└──────────────────────┴────────────────────────────────────────────────────────────────────────────────────┴──────────────┘
Run Time: real 14.919 user 14.912000 sys 0.008000
D SELECT UserID, extract(minute FROM EventTime) AS m, SearchPhrase, count(*) FROM hits GROUP BY UserID, m, SearchPhrase ORDER BY count(*) DESC LIMIT 10;W
Run Time: real 0.000 user 0.000000 sys 0.000000
Error: Binder Error: No function matches the given name and argument types 'date_part(VARCHAR, BIGINT)'. You might need to add explicit type casts.
        Candidate functions:
        date_part(VARCHAR, DATE) -> BIGINT
        date_part(VARCHAR, TIMESTAMP) -> BIGINT
        date_part(VARCHAR, TIME) -> BIGINT
        date_part(VARCHAR, INTERVAL) -> BIGINT

LINE 1: SELECT UserID, extract(minute FROM EventTime) AS m, Se...
                       ^
D SELECT UserID FROM hits WHERE UserID = -6101065172474983726;
Run Time: real 0.000 user 0.000000 sys 0.000000
Error: Conversion Error: Type INT64 with value -6101065172474983726 can't be cast because the value is out of range for the destination type UINT64
D SELECT count(*) FROM hits WHERE URL LIKE '%metrika%';
Run Time: real 0.000 user 0.000000 sys 0.000000
Error: Binder Error: No function matches the given name and argument types '~~(BLOB, VARCHAR)'. You might need to add explicit type casts.
        Candidate functions:
        ~~(VARCHAR, VARCHAR) -> BOOLEAN

D SELECT SearchPhrase, min(URL), count(*) AS c FROM hits WHERE URL LIKE '%metrika%' AND SearchPhrase != '' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10;A
Run Time: real 0.000 user 0.000000 sys 0.000000
Error: Binder Error: No function matches the given name and argument types '~~(BLOB, VARCHAR)'. You might need to add explicit type casts.
        Candidate functions:
        ~~(VARCHAR, VARCHAR) -> BOOLEAN

D , min(URL), min(Title), count(*) AS c, COUNT(DISTINCT UserID) FROM hits WHERE Title LIKE '%Яндекс%' AND URL NOT LIKE '%.yandex.%' AND SearchPhrase != '' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10;A
Run Time: real 0.000 user 0.000000 sys 0.000000
Error: Binder Error: No function matches the given name and argument types '~~(BLOB, VARCHAR)'. You might need to add explicit type casts.
        Candidate functions:
        ~~(VARCHAR, VARCHAR) -> BOOLEAN

D SELECT * FROM hits WHERE URL LIKE '%metrika%' ORDER BY EventTime LIMIT 10;
Run Time: real 0.000 user 0.000000 sys 0.000000
Error: Binder Error: No function matches the given name and argument types '~~(BLOB, VARCHAR)'. You might need to add explicit type casts.
        Candidate functions:
        ~~(VARCHAR, VARCHAR) -> BOOLEAN

D SELECT SearchPhrase FROM hits WHERE SearchPhrase != '' ORDER BY EventTime LIMIT 10;
┌────────────────────────────────────────────────────────────────────────────────────┐
│                                    SearchPhrase                                    │
├────────────────────────────────────────────────────────────────────────────────────┤
│ galaxy s4 zoom \xD1\x84\xD0\xB8\xD0\xBB\xD1\x8C\xD0\xBC                            │
│ \xD0\xBD\xD0\xBE\xD1\x87\xD0\xBD\xD0\xBE \xD0\xBA\xD0\xB8\xD1\x82\xD0\xB0\xD1\x... │
│ \xD1\x81\xD0\xB8\xD0\xBC\xD0\xBF\xD1\x82\xD0\xBE\xD0\xBC\xD1\x8B \xD1\x80\xD0\x... │
│ \xD1\x84\xD0\xB8\xD0\xBB\xD1\x8C\xD0\xBC \xD0\xBD\xD0\xB5\xD0\xB1\xD0\xBE\xD0\x... │
│ \xD1\x80\xD0\xB0\xD1\x81\xD0\xBF\xD0\xB8\xD1\x81\xD0\xB0\xD0\xBD\xD0\xB8\xD0\xB... │
│ \xD0\xB1\xD1\x80\xD0\xB8\xD1\x82\xD0\xB0 \xD0\xB3\xD0\xB0\xD0\xBD\xD0\xB0\xD0\x... │
│ \xD0\xB0\xD0\xBD\xD0\xB0\xD0\xBF\xD0\xB0 \xD0\xBE\xD0\xBF\xD0\xB5\xD1\x80\xD0\x... │
│ \xD1\x81\xD0\xBA\xD0\xB0\xD1\x87\xD0\xB0\xD1\x82\xD1\x8C \xD1\x87\xD0\xB8\xD1\x... │
│ \xD1\x81\xD0\xBB\xD0\xBE\xD0\xBD.\xD1\x80\xD1\x83\xD0\xB1., \xD0\xB4. \xD0\xB0.... │
│ \xD0\xBE\xD1\x82\xD0\xB4\xD1\x8B\xD1\x85\xD0\xB0 \xD1\x87\xD0\xB5\xD0\xBC \xD0\... │
└────────────────────────────────────────────────────────────────────────────────────┘
Run Time: real 4.282 user 3.572000 sys 0.048000
D SELECT SearchPhrase FROM hits WHERE SearchPhrase != '' ORDER BY SearchPhrase LIMIT 10;=
┌────────────────────────────────────────────────────────────────────────────────────┐
│                                    SearchPhrase                                    │
├────────────────────────────────────────────────────────────────────────────────────┤
│ ! hektdf gjcgjhn conster                                                           │
│ ! \xD1\x81\xD0\xBA\xD0\xB0\xD1\x80\xD0\xBF                                         │
│ !(\xD0\xBA\xD0\xB0\xD0\xBA \xD0\xB2\xD0\xBE\xD1\x80\xD0\xBE\xD0\xBD\xD0\xB8        │
│ !(\xD0\xBF\xD0\xBE \xD0\xB3\xD0\xBE\xD1\x80\xD0\xB8\xD1\x8E \xD0\xB2 \xD1\x8F\x... │
│ !(\xD1\x81) \xD0\xBF\xD1\x80\xD0\xBE \xD0\xB4\xD0\xBF\xD0\xBE \xD1\x81\xD0\xB5\... │
│ !(\xD1\x81\xD0\xB0\xD0\xBB\xD0\xBE\xD0\xBD\xD1\x8B \xD0\xBE\xD1\x81\xD1\x82\xD0... │
│ !(\xD1\x81\xD1\x82\xD0\xB0\xD1\x80\xD1\x82\xD0\xB5\xD1\x80 rav4 \xD1\x82\xD1\x8... │
│ !\xD0\xBA\xD1\x83\xD0\xB3\xD0\xB8 \xD0\xB4\xD0\xBB\xD1\x8F \xD0\xBC\xD1\x8F\xD1... │
│ !\xD0\xBA\xD1\x83\xD0\xB3\xD0\xB8 \xD0\xBC\xD0\xB0\xD1\x83\xD1\x81 \xD0\xBA\xD0... │
│ !\xD0\xBA\xD1\x83\xD0\xB3\xD0\xB8 \xD1\x81\xD0\xB5\xD1\x80\xD0\xB8\xD0\xB8         │
└────────────────────────────────────────────────────────────────────────────────────┘
Run Time: real 3.610 user 3.612000 sys 0.000000
D SELECT SearchPhrase FROM hits WHERE SearchPhrase != '' ORDER BY EventTime, SearchPhrase LIMIT 10;
┌────────────────────────────────────────────────────────────────────────────────────┐
│                                    SearchPhrase                                    │
├────────────────────────────────────────────────────────────────────────────────────┤
│ galaxy s4 zoom \xD1\x84\xD0\xB8\xD0\xBB\xD1\x8C\xD0\xBC                            │
│ \xD0\xBD\xD0\xBE\xD1\x87\xD0\xBD\xD0\xBE \xD0\xBA\xD0\xB8\xD1\x82\xD0\xB0\xD1\x... │
│ \xD1\x81\xD0\xB8\xD0\xBC\xD0\xBF\xD1\x82\xD0\xBE\xD0\xBC\xD1\x8B \xD1\x80\xD0\x... │
│ \xD1\x84\xD0\xB8\xD0\xBB\xD1\x8C\xD0\xBC \xD0\xBD\xD0\xB5\xD0\xB1\xD0\xBE\xD0\x... │
│ \xD0\xB0\xD0\xB2\xD0\xBE\xD0\xBC \xD0\xBA\xD0\xBE\xD0\xBD\xD1\x81\xD1\x82\xD0\x... │
│ \xD0\xB0\xD0\xBD\xD0\xB0\xD0\xBF\xD0\xB0 \xD0\xBE\xD0\xBF\xD0\xB5\xD1\x80\xD0\x... │
│ \xD0\xB1\xD1\x80\xD0\xB8\xD1\x82\xD0\xB0 \xD0\xB3\xD0\xB0\xD0\xBD\xD0\xB0\xD0\x... │
│ \xD0\xBA\xD0\xBE\xD0\xBC\xD0\xBF\xD1\x8C\xD1\x8E\xD1\x82\xD0\xB5\xD1\x80\xD0\xB... │
│ \xD0\xBE\xD1\x82\xD0\xB4\xD1\x8B\xD1\x85\xD0\xB0 \xD1\x87\xD0\xB5\xD0\xBC \xD0\... │
│ \xD1\x80\xD0\xB0\xD1\x81\xD0\xBF\xD0\xB8\xD1\x81\xD0\xB0\xD0\xBD\xD0\xB8\xD0\xB... │
└────────────────────────────────────────────────────────────────────────────────────┘
Run Time: real 3.640 user 3.640000 sys 0.000000
D SELECT CounterID, avg(length(URL)) AS l, count(*) AS c FROM hits WHERE URL != '' GROUP BY CounterID HAVING count(*) > 100000 ORDER BY l DESC LIMIT 25;
Run Time: real 0.000 user 0.000000 sys 0.000000
Error: Binder Error: No function matches the given name and argument types 'length(BLOB)'. You might need to add explicit type casts.
        Candidate functions:
        length(VARCHAR) -> BIGINT
        length(LIST) -> BIGINT

LINE 1: SELECT CounterID, avg(length(URL)) AS l, count(*) AS c FROM h...
                              ^
D Referer, '^https?://(?:www\.)?([^/]+)/.*$', '\1') AS key, avg(length(Referer)) AS l, count(*) AS c, min(Referer) FROM hits WHERE Referer != '' GROUP BY key HAVING count(*) > 100000 ORDER BY l DESC LIMIT 25;
VRun Time: real 0.000 user 0.000000 sys 0.000000
Error: Binder Error: No function matches the given name and argument types 'regexp_replace(BLOB, VARCHAR, VARCHAR)'. You might need to add explicit type casts.
        Candidate functions:
        regexp_replace(VARCHAR, VARCHAR, VARCHAR) -> VARCHAR
        regexp_replace(VARCHAR, VARCHAR, VARCHAR, VARCHAR) -> VARCHAR

LINE 1: SELECT REGEXP_REPLACE(Referer, '^https?://(?:w...
               ^
D  + 82), sum(ResolutionWidth + 83), sum(ResolutionWidth + 84), sum(ResolutionWidth + 85), sum(ResolutionWidth + 86), sum(ResolutionWidth + 87), sum(ResolutionWidth + 88), sum(ResolutionWidth + 89) FROM hits;
┌──────────────────────┬──────────────────────────┬──────────────────────────┬──────────────────────────┬──────────────────────────┬──────────────────────────┬──────────────────────────┬──────────────────────────┬──────────────────────────┬──────────────────────────┬───────────────────────────┬───────────────────────────┬───────────────────────────┬───────────────────────────┬───────────────────────────┬───────────────────────────┬───────────────────────────┬───────────────────────────┬───────────────────────────┬───────────────────────────┬───────────────────────────┬───────────────────────────┬───────────────────────────┬───────────────────────────┬───────────────────────────┬───────────────────────────┬───────────────────────────┬───────────────────────────┬───────────────────────────┬───────────────────────────┬───────────────────────────┬───────────────────────────┬───────────────────────────┬───────────────────────────┬───────────────────────────┬───────────────────────────┬───────────────────────────┬───────────────────────────┬───────────────────────────┬───────────────────────────┬───────────────────────────┬───────────────────────────┬───────────────────────────┬───────────────────────────┬───────────────────────────┬───────────────────────────┬───────────────────────────┬───────────────────────────┬───────────────────────────┬───────────────────────────┬───────────────────────────┬───────────────────────────┬───────────────────────────┬───────────────────────────┬───────────────────────────┬───────────────────────────┬───────────────────────────┬───────────────────────────┬───────────────────────────┬───────────────────────────┬───────────────────────────┬───────────────────────────┬───────────────────────────┬───────────────────────────┬───────────────────────────┬───────────────────────────┬───────────────────────────┬───────────────────────────┬───────────────────────────┬───────────────────────────┬───────────────────────────┬───────────────────────────┬───────────────────────────┬───────────────────────────┬───────────────────────────┬───────────────────────────┬───────────────────────────┬───────────────────────────┬───────────────────────────┬───────────────────────────┬───────────────────────────┬───────────────────────────┬───────────────────────────┬───────────────────────────┬───────────────────────────┬───────────────────────────┬───────────────────────────┬───────────────────────────┬───────────────────────────┬───────────────────────────┐
│ sum(resolutionwidth) │ sum(resolutionwidth + 1) │ sum(resolutionwidth + 2) │ sum(resolutionwidth + 3) │ sum(resolutionwidth + 4) │ sum(resolutionwidth + 5) │ sum(resolutionwidth + 6) │ sum(resolutionwidth + 7) │ sum(resolutionwidth + 8) │ sum(resolutionwidth + 9) │ sum(resolutionwidth + 10) │ sum(resolutionwidth + 11) │ sum(resolutionwidth + 12) │ sum(resolutionwidth + 13) │ sum(resolutionwidth + 14) │ sum(resolutionwidth + 15) │ sum(resolutionwidth + 16) │ sum(resolutionwidth + 17) │ sum(resolutionwidth + 18) │ sum(resolutionwidth + 19) │ sum(resolutionwidth + 20) │ sum(resolutionwidth + 21) │ sum(resolutionwidth + 22) │ sum(resolutionwidth + 23) │ sum(resolutionwidth + 24) │ sum(resolutionwidth + 25) │ sum(resolutionwidth + 26) │ sum(resolutionwidth + 27) │ sum(resolutionwidth + 28) │ sum(resolutionwidth + 29) │ sum(resolutionwidth + 30) │ sum(resolutionwidth + 31) │ sum(resolutionwidth + 32) │ sum(resolutionwidth + 33) │ sum(resolutionwidth + 34) │ sum(resolutionwidth + 35) │ sum(resolutionwidth + 36) │ sum(resolutionwidth + 37) │ sum(resolutionwidth + 38) │ sum(resolutionwidth + 39) │ sum(resolutionwidth + 40) │ sum(resolutionwidth + 41) │ sum(resolutionwidth + 42) │ sum(resolutionwidth + 43) │ sum(resolutionwidth + 44) │ sum(resolutionwidth + 45) │ sum(resolutionwidth + 46) │ sum(resolutionwidth + 47) │ sum(resolutionwidth + 48) │ sum(resolutionwidth + 49) │ sum(resolutionwidth + 50) │ sum(resolutionwidth + 51) │ sum(resolutionwidth + 52) │ sum(resolutionwidth + 53) │ sum(resolutionwidth + 54) │ sum(resolutionwidth + 55) │ sum(resolutionwidth + 56) │ sum(resolutionwidth + 57) │ sum(resolutionwidth + 58) │ sum(resolutionwidth + 59) │ sum(resolutionwidth + 60) │ sum(resolutionwidth + 61) │ sum(resolutionwidth + 62) │ sum(resolutionwidth + 63) │ sum(resolutionwidth + 64) │ sum(resolutionwidth + 65) │ sum(resolutionwidth + 66) │ sum(resolutionwidth + 67) │ sum(resolutionwidth + 68) │ sum(resolutionwidth + 69) │ sum(resolutionwidth + 70) │ sum(resolutionwidth + 71) │ sum(resolutionwidth + 72) │ sum(resolutionwidth + 73) │ sum(resolutionwidth + 74) │ sum(resolutionwidth + 75) │ sum(resolutionwidth + 76) │ sum(resolutionwidth + 77) │ sum(resolutionwidth + 78) │ sum(resolutionwidth + 79) │ sum(resolutionwidth + 80) │ sum(resolutionwidth + 81) │ sum(resolutionwidth + 82) │ sum(resolutionwidth + 83) │ sum(resolutionwidth + 84) │ sum(resolutionwidth + 85) │ sum(resolutionwidth + 86) │ sum(resolutionwidth + 87) │ sum(resolutionwidth + 88) │ sum(resolutionwidth + 89) │
├──────────────────────┼──────────────────────────┼──────────────────────────┼──────────────────────────┼──────────────────────────┼──────────────────────────┼──────────────────────────┼──────────────────────────┼──────────────────────────┼──────────────────────────┼───────────────────────────┼───────────────────────────┼───────────────────────────┼───────────────────────────┼───────────────────────────┼───────────────────────────┼───────────────────────────┼───────────────────────────┼───────────────────────────┼───────────────────────────┼───────────────────────────┼───────────────────────────┼───────────────────────────┼───────────────────────────┼───────────────────────────┼───────────────────────────┼───────────────────────────┼───────────────────────────┼───────────────────────────┼───────────────────────────┼───────────────────────────┼───────────────────────────┼───────────────────────────┼───────────────────────────┼───────────────────────────┼───────────────────────────┼───────────────────────────┼───────────────────────────┼───────────────────────────┼───────────────────────────┼───────────────────────────┼───────────────────────────┼───────────────────────────┼───────────────────────────┼───────────────────────────┼───────────────────────────┼───────────────────────────┼───────────────────────────┼───────────────────────────┼───────────────────────────┼───────────────────────────┼───────────────────────────┼───────────────────────────┼───────────────────────────┼───────────────────────────┼───────────────────────────┼───────────────────────────┼───────────────────────────┼───────────────────────────┼───────────────────────────┼───────────────────────────┼───────────────────────────┼───────────────────────────┼───────────────────────────┼───────────────────────────┼───────────────────────────┼───────────────────────────┼───────────────────────────┼───────────────────────────┼───────────────────────────┼───────────────────────────┼───────────────────────────┼───────────────────────────┼───────────────────────────┼───────────────────────────┼───────────────────────────┼───────────────────────────┼───────────────────────────┼───────────────────────────┼───────────────────────────┼───────────────────────────┼───────────────────────────┼───────────────────────────┼───────────────────────────┼───────────────────────────┼───────────────────────────┼───────────────────────────┼───────────────────────────┼───────────────────────────┼───────────────────────────┤
│ 151348908394         │ 151448908394             │ 151548908394             │ 151648908394             │ 151748908394             │ 151848908394             │ 151948908394             │ 152048908394             │ 152148908394             │ 152248908394             │ 152348908394              │ 152448908394              │ 152548908394              │ 152648908394              │ 152748908394              │ 152848908394              │ 152948908394              │ 153048908394              │ 153148908394              │ 153248908394              │ 153348908394              │ 153448908394              │ 153548908394              │ 153648908394              │ 153748908394              │ 153848908394              │ 153948908394              │ 154048908394              │ 154148908394              │ 154248908394              │ 154348908394              │ 154448908394              │ 154548908394              │ 154648908394              │ 154748908394              │ 154848908394              │ 154948908394              │ 155048908394              │ 155148908394              │ 155248908394              │ 155348908394              │ 155448908394              │ 155548908394              │ 155648908394              │ 155748908394              │ 155848908394              │ 155948908394              │ 156048908394              │ 156148908394              │ 156248908394              │ 156348908394              │ 156448908394              │ 156548908394              │ 156648908394              │ 156748908394              │ 156848908394              │ 156948908394              │ 157048908394              │ 157148908394              │ 157248908394              │ 157348908394              │ 157448908394              │ 157548908394              │ 157648908394              │ 157748908394              │ 157848908394              │ 157948908394              │ 158048908394              │ 158148908394              │ 158248908394              │ 158348908394              │ 158448908394              │ 158548908394              │ 158648908394              │ 158748908394              │ 158848908394              │ 158948908394              │ 159048908394              │ 159148908394              │ 159248908394              │ 159348908394              │ 159448908394              │ 159548908394              │ 159648908394              │ 159748908394              │ 159848908394              │ 159948908394              │ 160048908394              │ 160148908394              │ 160248908394              │
└──────────────────────┴──────────────────────────┴──────────────────────────┴──────────────────────────┴──────────────────────────┴──────────────────────────┴──────────────────────────┴──────────────────────────┴──────────────────────────┴──────────────────────────┴───────────────────────────┴───────────────────────────┴───────────────────────────┴───────────────────────────┴───────────────────────────┴───────────────────────────┴───────────────────────────┴───────────────────────────┴───────────────────────────┴───────────────────────────┴───────────────────────────┴───────────────────────────┴───────────────────────────┴───────────────────────────┴───────────────────────────┴───────────────────────────┴───────────────────────────┴───────────────────────────┴───────────────────────────┴───────────────────────────┴───────────────────────────┴───────────────────────────┴───────────────────────────┴───────────────────────────┴───────────────────────────┴───────────────────────────┴───────────────────────────┴───────────────────────────┴───────────────────────────┴───────────────────────────┴───────────────────────────┴───────────────────────────┴───────────────────────────┴───────────────────────────┴───────────────────────────┴───────────────────────────┴───────────────────────────┴───────────────────────────┴───────────────────────────┴───────────────────────────┴───────────────────────────┴───────────────────────────┴───────────────────────────┴───────────────────────────┴───────────────────────────┴───────────────────────────┴───────────────────────────┴───────────────────────────┴───────────────────────────┴───────────────────────────┴───────────────────────────┴───────────────────────────┴───────────────────────────┴───────────────────────────┴───────────────────────────┴───────────────────────────┴───────────────────────────┴───────────────────────────┴───────────────────────────┴───────────────────────────┴───────────────────────────┴───────────────────────────┴───────────────────────────┴───────────────────────────┴───────────────────────────┴───────────────────────────┴───────────────────────────┴───────────────────────────┴───────────────────────────┴───────────────────────────┴───────────────────────────┴───────────────────────────┴───────────────────────────┴───────────────────────────┴───────────────────────────┴───────────────────────────┴───────────────────────────┴───────────────────────────┴───────────────────────────┴───────────────────────────┘
Run Time: real 85.256 user 85.252000 sys 0.000000
D SELECT SearchEngineID, ClientIP, count(*) AS c, sum("refresh"), avg(ResolutionWidth) FROM hits WHERE SearchPhrase != '' GROUP BY SearchEngineID, ClientIP ORDER BY c DESC LIMIT 10;
┌────────────────┬────────────┬──────┬──────────────┬──────────────────────┐
│ SearchEngineID │  ClientIP  │  c   │ sum(refresh) │ avg(resolutionwidth) │
├────────────────┼────────────┼──────┼──────────────┼──────────────────────┤
│ 2              │ 1138507705 │ 1633 │ 35           │ 1408.0122473974282   │
│ 2              │ 1740861572 │ 1331 │ 28           │ 1577.945905334335    │
│ 2              │ 3487820196 │ 1144 │ 35           │ 1553.1984265734266   │
│ 2              │ 3797060577 │ 1140 │ 36           │ 1543.4140350877192   │
│ 2              │ 2349209741 │ 1105 │ 30           │ 1557.387330316742    │
│ 2              │ 2424344199 │ 1102 │ 31           │ 1555.6588021778584   │
│ 2              │ 3663904793 │ 1083 │ 31           │ 1581.8171745152354   │
│ 2              │ 3829154130 │ 1082 │ 30           │ 1541.253234750462    │
│ 2              │ 2551371145 │ 1080 │ 24           │ 1559.8092592592593   │
│ 2              │ 4029049820 │ 1058 │ 32           │ 1556.2003780718337   │
└────────────────┴────────────┴──────┴──────────────┴──────────────────────┘
Run Time: real 8.033 user 7.032000 sys 0.048000
D SELECT WatchID, ClientIP, count(*) AS c, sum("refresh"), avg(ResolutionWidth) FROM hits WHERE SearchPhrase != '' GROUP BY WatchID, ClientIP ORDER BY c DESC LIMIT 10;
┌─────────────────────┬────────────┬───┬──────────────┬──────────────────────┐
│       WatchID       │  ClientIP  │ c │ sum(refresh) │ avg(resolutionwidth) │
├─────────────────────┼────────────┼───┼──────────────┼──────────────────────┤
│ 7472773096904766158 │ 972408088  │ 2 │ 0            │ 1368.0               │
│ 8515267528803597958 │ 2005721512 │ 2 │ 0            │ 1917.0               │
│ 5431383378337214900 │ 1373018819 │ 2 │ 0            │ 1087.0               │
│ 4975771741728931240 │ 1594850068 │ 2 │ 0            │ 1917.0               │
│ 6143560365929503526 │ 2912060982 │ 2 │ 0            │ 1368.0               │
│ 4661775965756901134 │ 3822464671 │ 2 │ 0            │ 1638.0               │
│ 5340100429706330950 │ 709893659  │ 2 │ 0            │ 1368.0               │
│ 5265600775603767970 │ 1677655885 │ 2 │ 0            │ 1396.0               │
│ 5449946953533528811 │ 3822667196 │ 2 │ 0            │ 1638.0               │
│ 6426552621243022389 │ 3557962159 │ 2 │ 0            │ 1638.0               │
└─────────────────────┴────────────┴───┴──────────────┴──────────────────────┘
Run Time: real 9.317 user 8.380000 sys 0.052000
D SELECT WatchID, ClientIP, count(*) AS c, sum("refresh"), avg(ResolutionWidth) FROM hits GROUP BY WatchID, ClientIP ORDER BY c DESC LIMIT 10;
┌─────────────────────┬────────────┬───┬──────────────┬──────────────────────┐
│       WatchID       │  ClientIP  │ c │ sum(refresh) │ avg(resolutionwidth) │
├─────────────────────┼────────────┼───┼──────────────┼──────────────────────┤
│ 5732691047654519103 │ 1097532796 │ 2 │ 0            │ 1638.0               │
│ 8308952461884454508 │ 2609801721 │ 2 │ 0            │ 1087.0               │
│ 7472773096904766158 │ 972408088  │ 2 │ 0            │ 1368.0               │
│ 7360470262372840837 │ 972408088  │ 2 │ 0            │ 1368.0               │
│ 4778976465399160621 │ 3938580212 │ 2 │ 2            │ 1638.0               │
│ 4848145794958638974 │ 3938580212 │ 2 │ 0            │ 1638.0               │
│ 9172448021081089285 │ 2530876984 │ 2 │ 0            │ 1638.0               │
│ 6471985135199404171 │ 765833715  │ 2 │ 0            │ 1594.0               │
│ 8824813183119863159 │ 765833715  │ 2 │ 0            │ 1594.0               │
│ 8227322756510819845 │ 765833715  │ 2 │ 0            │ 1594.0               │
└─────────────────────┴────────────┴───┴──────────────┴──────────────────────┘
Run Time: real 48.016 user 32.076000 sys 8.092000
D SELECT URL, count(*) AS c FROM hits GROUP BY URL ORDER BY c DESC LIMIT 10;
┌────────────────────────────────────────────────────────────────────────────────────┬─────────┐
│                                        URL                                         │    c    │
├────────────────────────────────────────────────────────────────────────────────────┼─────────┤
│ http://liver.ru/belgorod/page/1006.j\xD0\xBA\xD0\xB8/\xD0\xB4\xD0\xBE\xD0\xBF_\... │ 3288173 │
│ http://kinopoisk.ru                                                                │ 1625251 │
│ http://bdsm_po_yers=0&with_video                                                   │ 791465  │
│ http://video.yandex                                                                │ 582404  │
│ http://smeshariki.ru/region                                                        │ 514984  │
│ http://auto_fiat_dlya-bluzki%2F8536.30.18&he=900&with                              │ 507995  │
│ http://liver.ru/place_rukodel=365115eb7bbb90                                       │ 359893  │
│ http://kinopoisk.ru/vladimir.irr.ru                                                │ 354690  │
│ http://video.yandex.ru/search/?jenre=50&s_yers                                     │ 318979  │
│ http://tienskaia-moda                                                              │ 289355  │
└────────────────────────────────────────────────────────────────────────────────────┴─────────┘
Run Time: real 55.180 user 33.916000 sys 1.012000
D SELECT 1, URL, count(*) AS c FROM hits GROUP BY 1, URL ORDER BY c DESC LIMIT 10;
┌───┬────────────────────────────────────────────────────────────────────────────────────┬─────────┐
│ 1 │                                        URL                                         │    c    │
├───┼────────────────────────────────────────────────────────────────────────────────────┼─────────┤
│ 1 │ http://liver.ru/belgorod/page/1006.j\xD0\xBA\xD0\xB8/\xD0\xB4\xD0\xBE\xD0\xBF_\... │ 3288173 │
│ 1 │ http://kinopoisk.ru                                                                │ 1625251 │
│ 1 │ http://bdsm_po_yers=0&with_video                                                   │ 791465  │
│ 1 │ http://video.yandex                                                                │ 582404  │
│ 1 │ http://smeshariki.ru/region                                                        │ 514984  │
│ 1 │ http://auto_fiat_dlya-bluzki%2F8536.30.18&he=900&with                              │ 507995  │
│ 1 │ http://liver.ru/place_rukodel=365115eb7bbb90                                       │ 359893  │
│ 1 │ http://kinopoisk.ru/vladimir.irr.ru                                                │ 354690  │
│ 1 │ http://video.yandex.ru/search/?jenre=50&s_yers                                     │ 318979  │
│ 1 │ http://tienskaia-moda                                                              │ 289355  │
└───┴────────────────────────────────────────────────────────────────────────────────────┴─────────┘
Run Time: real 34.194 user 34.132000 sys 0.060000
D SELECT ClientIP, ClientIP - 1, ClientIP - 2, ClientIP - 3, count(*) AS c FROM hits GROUP BY ClientIP, ClientIP - 1, ClientIP - 2, ClientIP - 3 ORDER BY c DESC LIMIT 10;
┌────────────┬──────────────┬──────────────┬──────────────┬───────┐
│  ClientIP  │ clientip - 1 │ clientip - 2 │ clientip - 3 │   c   │
├────────────┼──────────────┼──────────────┼──────────────┼───────┤
│ 4255045322 │ 4255045321   │ 4255045320   │ 4255045319   │ 47008 │
│ 2596862839 │ 2596862838   │ 2596862837   │ 2596862836   │ 29121 │
│ 3119147744 │ 3119147743   │ 3119147742   │ 3119147741   │ 25333 │
│ 1696638182 │ 1696638181   │ 1696638180   │ 1696638179   │ 20230 │
│ 1138507705 │ 1138507704   │ 1138507703   │ 1138507702   │ 15778 │
│ 3367941774 │ 3367941773   │ 3367941772   │ 3367941771   │ 12768 │
│ 3032827420 │ 3032827419   │ 3032827418   │ 3032827417   │ 11349 │
│ 1740861572 │ 1740861571   │ 1740861570   │ 1740861569   │ 11315 │
│ 3487820196 │ 3487820195   │ 3487820194   │ 3487820193   │ 9881  │
│ 3663904793 │ 3663904792   │ 3663904791   │ 3663904790   │ 9718  │
└────────────┴──────────────┴──────────────┴──────────────┴───────┘
Run Time: real 17.028 user 17.024000 sys 0.004000
D ) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND DontCountHits = 0 AND "refresh" = 0 AND URL != '' GROUP BY URL ORDER BY PageViews DESC LIMIT 10;
Run Time: real 0.000 user 0.000000 sys 0.000000
Error: Conversion Error: Could not convert string '2013-07-01' to UINT16
D  PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND DontCountHits = 0 AND "refresh" = 0 AND Title != '' GROUP BY Title ORDER BY PageViews DESC LIMIT 10;
Run Time: real 0.001 user 0.000000 sys 0.000000
Error: Conversion Error: Could not convert string '2013-07-01' to UINT16
D  AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND "refresh" = 0 AND IsLink != 0 AND IsDownload = 0 GROUP BY URL ORDER BY PageViews DESC LIMIT 1000;
Run Time: real 0.000 user 0.000000 sys 0.000000
Error: Conversion Error: Could not convert string '2013-07-01' to UINT16
D ROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND "refresh" = 0 GROUP BY TraficSourceID, SearchEngineID, AdvEngineID, Src, Dst ORDER BY PageViews DESC LIMIT 1000;
Run Time: real 0.001 user 0.004000 sys 0.000000
Error: Conversion Error: Could not convert string '2013-07-01' to UINT16
D ND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND "refresh" = 0 AND TraficSourceID IN (-1, 6) AND RefererHash = 686716256552154761 GROUP BY URLHash, EventDate ORDER BY PageViews DESC LIMIT 100;
Run Time: real 0.000 user 0.000000 sys 0.000000
Error: Conversion Error: Could not convert string '2013-07-01' to UINT16
D ate >= '2013-07-01' AND EventDate <= '2013-07-31' AND "refresh" = 0 AND DontCountHits = 0 AND URLHash = 686716256552154761 GROUP BY WindowClientWidth, WindowClientHeight ORDER BY PageViews DESC LIMIT 10000;
Run Time: real 0.000 user 0.000000 sys 0.000000
Error: Conversion Error: Could not convert string '2013-07-01' to UINT16
D ts WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-02' AND "refresh" = 0 AND DontCountHits = 0 GROUP BY DATE_TRUNC('minute', EventTime) ORDER BY DATE_TRUNC('minute', EventTime);
Run Time: real 0.000 user 0.000000 sys 0.000000
Error: Binder Error: No function matches the given name and argument types 'date_trunc(VARCHAR, BIGINT)'. You might need to add explicit type casts.
        Candidate functions:
        date_trunc(VARCHAR, TIMESTAMP) -> TIMESTAMP
        date_trunc(VARCHAR, DATE) -> TIMESTAMP

LINE 1: ...sh" = 0 AND DontCountHits = 0 GROUP BY DATE_TRUNC('minute', EventTime) ORDER B...
```

Mostly alright but some functions are missing and the types from Parquet are wrong.

Let's try to load from CSV:

```
SELECT * FROM hits_100m_obfuscated INTO OUTFILE 'hits.csv' FORMAT CSV
```

```
$ ./duckdb
v0.3.0 46a0fc50a
Enter ".help" for usage hints.
Connected to a transient in-memory database.
Use ".open FILENAME" to reopen on a persistent database.
D .open 'duckdb.db'
D ;
D PRAGMA temp_directory='duckdb.tmp';
Error: Parser Error: syntax error at or near ""
LINE 1: PRAGMA temp_directory='duckdb.tmp';
        ^
D PRAGMA temp_directory='duckdb.tmp';
Error: Parser Error: syntax error at or near ""
LINE 1: PRAGMA temp_directory='duckdb.tmp';
        ^
D .open 'duckdb.db';
D
D SELECT 1
> ;
┌───┐
│ 1 │
├───┤
│ 1 │
└───┘
D PRAGMA temp_directory='duckdb.tmp';
Error: Parser Error: syntax error at or near ""
LINE 1: PRAGMA temp_directory='duckdb.tmp';
        ^
D CREATE TABLE hits AS SELECT * FROM read_csv_auto('hits.csv');
Error: String value is not valid UTF8
```

It does not work for non-UTF8 data.

Let's cleanup UTF-8.

```
clickhouse-local --input-format LineAsString --output-format TSVRaw --structure 's String' --query "SELECT toValidUTF8(s) FROM table" --progress < hits.csv > hits_valid.csv
```

```
D CREATE TABLE hits AS SELECT * FROM read_csv_auto('hits_valid.csv');
Error: Invalid Input Error: Could not convert string '2149615427' to INT32 in column "column082", between line 137217 and 138240. Parser options: DELIMITER=',' (auto detected), QUOTE='"' (auto detected), ESCAPE='' (auto detected), HEADER=0 (auto detected), SAMPLE_SIZE=10240, ALL_VARCHAR=0. Consider either increasing the sample size (SAMPLE_SIZE=X [X rows] or SAMPLE_SIZE=-1 [all rows]), or skipping column conversion (ALL_VARCHAR=1)
```

Does not work either.

DuckDB CLI does not support history search (Ctrl+R).

If I write a command and then prepend `-- ` before it, then history navigation becomes completely broken.

```
D CREATE TABLE hits AS SELECT * FROM parquet_scan('hits.parquet' ;
Run Time: real 0.000 user 0.000000 sys 0.000000
Error: Parser Error: syntax error at or near ";"
LINE 1: ...ECT * FROM parquet_scan('hits.parquet' ;
                                                  ^
D CREATE TABLE hits AS SELECT * FROM parquet_scan('hits.parque)' ;
```

```
D CREATE TABLE hits AS SELECT * FROM parquet_scan('hits.parquet');
Run Time: real 1086.631 user 758.036000 sys 201.360000
```

It's just about 100 000 rows/second. Quite decent but not good.

Cancelling queries by Ctrl+C does not work.

I've noticed that DuckDB is using single thread.
Most likely it can be fixed with

```
PRAGMA threads = 16;
```

```
PRAGMA threads = 16;
PRAGMA temp_directory='duckdb.tmp';
.timer on
CREATE TABLE hits AS SELECT * FROM parquet_scan('hits.parquet');
```

It's much better with threads:

```
D CREATE TABLE hits AS SELECT * FROM parquet_scan('hits.parquet');
Run Time: real 315.828 user 447.896000 sys 126.552000
```

Let's run all the queries three times.

Collecting the results:

```
grep -F 'Run Time: real ' log | grep -oP 'real [\d\.]+' | grep -oP '[\d\.]+' | tail -n129 | clickhouse-local --structure 'x Decimal32(3)' --query "SELECT groupArray(x) FROM table GROUP BY rowNumberInAllBlocks() % 43 AS n ORDER BY n"
```

Let's create index to speed up point queries:

```
D CREATE INDEX counter_id_idx ON hits (CounterID);
Run Time: real 18.194 user 16.256000 sys 0.092000
```

Ok, it's quite fast.

Rerun the last 7 queries.
