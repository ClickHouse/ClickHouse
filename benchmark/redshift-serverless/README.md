This benchmark is not automated.

Go to AWS Redshift service.
Try Redshift Serverless. Use the default configuration.
The cluster will take a few minutes to start.
Go to "Query Editor". Establishing a connection takes around 10 seconds.

Run the CREATE TABLE statement you find in `create.sql`.

Note: Redshift prefers VARCHAR(MAX) instead of TEXT.

Then press on the "Load data".
This will generate a statement:

```
COPY dev.public.hits FROM 's3://clickhouse-public-datasets/hits_compatible/hits.csv.gz' GZIP
IAM_ROLE 'arn:aws:iam::...:role/service-role/AmazonRedshift-CommandsAccessRole-...'
FORMAT AS CSV DELIMITER ',' QUOTE '"'
REGION AS 'eu-central-1'
```

> Elapsed time: 32m 13.7s
 
It also have run 2380 "queries" for this task. 

Namespace configuration,
General Information, Storage used:

30.3 GB

Change admin user password:
dev, fGH4{dbas7

It's very difficult to find how to connect to it:
https://docs.aws.amazon.com/redshift/latest/mgmt/serverless-connecting.html

We will run the queries from another server with `psql` client.

```
sudo apt-get install -y postgresql-client

echo "*:*:*:*:your_password" > .pgpass
chmod 400 .pgpass

psql -h default.111111111111.eu-central-1.redshift-serverless.amazonaws.com -U dev -d dev -p 5439
```

Then run the benchmark:
```
export HOST=...
./run.sh 2>&1 | tee log.txt

cat log.txt | grep -oP 'Time: \d+\.\d+ ms|ERROR' | sed -r -e 's/Time: ([0-9]+\.[0-9]+) ms/\1/' |
  awk '{ if ($1 == "ERROR") { skip = 1 } else { if (i % 3 == 0) { printf "[" }; printf skip ? "null" : ($1 / 1000); if (i % 3 != 2) { printf "," } else { print "]," }; ++i; skip = 0; } }'
```
