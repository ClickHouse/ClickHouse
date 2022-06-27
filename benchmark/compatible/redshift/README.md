This benchmark is not automated.

Go to AWS Redshift service.
Create a cluster. Note: this is a classic Redshift, not "serverless".

Choose the node type and cluster size.
I've selected 4 nodes of ra3.xplus 4vCPU to get 16vCPU in total.

Set up some password for the admin user.
The cluster will take a few minutes to start.

We need to perform two modifications:
1. Allow inbound access. Go to VPC and edit the security group. Modify inbound rules. Allow connections from any IPv4 to port 5439.
2. Add IAM role. Just create something by default.

To create a table, you can go to the Query Editor v2.
Open the "dev" database.
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

> Elapsed time: 35m 35.9s

We will run the queries from another server with `psql` client.

```
sudo apt-get install -y postgresql-client

echo "*:*:*:*:your_password" > .pgpass
chmod 400 .pgpass

psql -h redshift-cluster-1.chedgchbam32.eu-central-1.redshift.amazonaws.com -U awsuser -d dev -p 5439
```

Then run the benchmark:
```
export HOST=...
./run.sh 2>&1 | tee log.txt


```

`SELECT sum(used * 1048576) FROM stv_node_storage_capacity`

> 30 794 579 968 
