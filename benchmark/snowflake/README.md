Snowflake recently removed the DeWitt Clause, so we are allowed to make benchmarks.

> Customer may conduct benchmark tests of the Service (each a “Test”). Other than with respect to Tests involving Previews, which may not be disclosed externally, Customer may externally disclose a Test or otherwise cause the results of a Test to be externally disclosed if it includes as part of the disclosure all information necessary to replicate the Test.

https://www.snowflake.com/legal/acceptable-use-policy/

Account setup took only 3 seconds.

Data -> Databases -> + Database
Database 'test' created.
Press on "public" schema.

Create table "standard".
Paste "create.sql".
Press on "create table" again.

Press on "admin", "warehouses", + Warehouse
The choice of a warehouse size is unclear. Let's choose X-Large by default.
It is using "credits" for pricing.

Set up SnowSQL.

```
curl -O https://sfc-repo.snowflakecomputing.com/snowsql/bootstrap/1.2/linux_x86_64/snowsql-1.2.22-linux_x86_64.bash
bash snowsql-1.2.22-linux_x86_64.bash
source .profile
```

Upload the data:

```
COPY INTO test.public.hits2 FROM 's3://clickhouse-public-datasets/hits_compatible/hits.csv.gz' FILE_FORMAT = (TYPE = CSV, COMPRESSION = GZIP, FIELD_OPTIONALLY_ENCLOSED_BY = '"')
```

42 min 4 sec.

```
export SNOWSQL_PWD='...' SNOWSQL_ACCOUNT='...' SNOWSQL_USER='myuser'

snowsql --region eu-central-1 --schemaname PUBLIC --dbname HITS --warehouse TEST --query "SELECT 1"
```

Before the benchmark:
```
ALTER USER myuser SET USE_CACHED_RESULT = false;
```

Run the benchmark:
```
./run.sh 2>&1 | tee log.txt

cat log.txt |
  grep -P 'Time Elapsed|^\d+ \(\w+\):' |
  sed -r -e 's/^[0-9]+ \([0-9A-Za-z]+\):.*$/null/; s/^.*Time Elapsed:\s*([0-9.]+)s$/\1/' |
  awk '{ if (i % 3 == 0) { printf "[" }; printf $1; if (i % 3 != 2) { printf "," } else { print "]," }; ++i; }'
```
