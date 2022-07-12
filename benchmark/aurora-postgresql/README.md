Select Aurora.
Select Aurora for PostgreSQL.
Select the latest version PostgreSQL 14.3.
Select Production template.

Database: database-2
User name: postgres
Master password: vci43A32#1

Select serverless.
16 minimum and maximum ACU (32 GB RAM).
Don't create an Aurora replica.
Public access: yes.
Turn off DevOps Guru.

Creation took around 15 seconds.
But creation of endpoints took longer (around 5..10 minutes).

Find the writer instance endpoint.
Example: database-1.cluster-cnkeohbxcwr1.eu-central-1.rds.amazonaws.com

```
sudo apt-get update
sudo apt-get install -y postgresql-client
```

Find "Security", click on the group in "VPC security groups".
Edit "Inbound rules". Add "Custom TCP", port 5432, from 0.0.0.0/0.

```
export HOST="database-2-instance-1.cnkeohbxcwr1.eu-central-1.rds.amazonaws.com"
echo "*:*:*:*:..." > .pgpass
chmod 400 .pgpass
```

Load the data

```
wget --continue 'https://datasets.clickhouse.com/hits_compatible/hits.tsv.gz'
gzip -d hits.tsv.gz

psql -U postgres -h "${HOST}" -t -c 'CREATE DATABASE test'
psql -U postgres -h "${HOST}" test -t < create.sql
psql -U postgres -h "${HOST}" test -t -c '\timing' -c "\\copy hits FROM 'hits.tsv'"
```

> COPY 99997497
> Time: 2126515.516 ms (35:26.516)

Go to "Monitoring", find "[Billed] Volume Bytes Used".

> 48.6 GiB

```
./run.sh 2>&1 | tee log.txt

cat log.txt | grep -oP 'Time: \d+\.\d+ ms' | sed -r -e 's/Time: ([0-9]+\.[0-9]+) ms/\1/' |
    awk '{ if (i % 3 == 0) { printf "[" }; printf $1 / 1000; if (i % 3 != 2) { printf "," } else { print "]," }; ++i; }'
```
