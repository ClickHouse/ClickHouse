The choice of a warehouse size is unclear. Let's choose X-Large by default.
It is using "credits" for pricing.

Storage cost: $23 USD per compressed TB per month
One credit is: $2.016/hour
X-Large: 16 credits/hour = $32/hour

It is very expensive, so let's touch it with a ten-foot pole and run away as quickly as possible.

Set up SnowSQL.

```
curl -O https://sfc-repo.snowflakecomputing.com/snowsql/bootstrap/1.2/linux_x86_64/snowsql-1.2.22-linux_x86_64.bash
bash snowsql-1.2.22-linux_x86_64.bash
source .profile
```

```
snowsql -a HA12345 -u USER
```

It does not connect after typing the password.

```
250001 (08001): Failed to connect to DB. Verify the account name is correct: HA12345.snowflakecomputing.com:443. 000403: 403: HTTP 403: Forbidden
If the error message is unclear, enable logging using -o log_level=DEBUG and see the log to find out the cause. Contact support for further help.
Goodbye!
```

It said "Goodbye!" in active-aggressive tone.

To know the account name, we have to go to the "classic console" and look at the URL in the browser.

> https://{this}.eu-central-1.snowflakecomputing.com/console/login?disableDirectLogin=true

But it does not help.

It works if I specify the region in the command line.
Although `snowsql --help` saying that it is DEPRECATED.

```
snowsql -a nn12345 -u USER --region eu-central-1 --schemaname PUBLIC --dbname TEST --warehouse TEST
```

Notes: SnowSQL is using autocomplete using well known Python library.
Autocomplete is not context-aware.

Upload the data:

```
put file:///home/ubuntu/hits.csv @test.public.%hits
```

The syntax is strange (all these @%#).
The query hung and did nothing.

Actually it is not hung. The snowsql is using 100% to parse CSV in Python for hours.

Let's try a different upload method.

```
COPY INTO test.public.hits2 FROM 's3://clickhouse-public-datasets/hits_compatible/hits.csv.gz' FILE_FORMAT = (TYPE = CSV, COMPRESSION = GZIP, FIELD_OPTIONALLY_ENCLOSED_BY = '"')
```

For some reason, it has selected X-Small warehouse, will need to change to X-Large.

42 min 4 sec.
