Select Aurora.
Select Aurora for MySQL.
Select the latest version 3.02.0 (compatible with MySQL 8.0.23)
Select Production template.

Database: database-1
User name: admin
Master password: vci43A32#1

Select serverless.
16 minimum and maximum ACU (32 GB RAM).
Don't create an Aurora replica.
Public access: yes.
Turn off DevOps Guru.

Creation took around 15 seconds.
But creation of endpoints took longer.

Find the writer instance endpoint.
Example: database-1.cluster-cnkeohbxcwr1.eu-central-1.rds.amazonaws.com

```
sudo apt-get update
sudo apt-get install -y mysql-client
```

Find "Security", click on the group in "VPC security groups".
Edit "Inbound rules". Add "Custom TCP", port 3306, from 0.0.0.0/0.

```
export HOST="database-1.cluster-cnkeohbxcwr1.eu-central-1.rds.amazonaws.com"
export PASSWORD="vci43A32#1"

mysql -h "${HOST}" -u admin --password="${PASSWORD}" -e "CREATE DATABASE test"
```

Load the data

```
wget --continue 'https://datasets.clickhouse.com/hits_compatible/hits.tsv.gz'
gzip -d hits.tsv.gz

mysql -h "${HOST}" -u admin --password="${PASSWORD}" test < create.sql

time mysql --local-infile=1 -h "${HOST}" -u admin --password="${PASSWORD}" test -e "LOAD DATA LOCAL INFILE 'hits.tsv' INTO TABLE hits"
```

> 128m7.318s

Go to "Monitoring", find "\[Billed\] Volume Bytes Used".

> 83.46 GiB

```
./run.sh 2>&1 | tee log.txt

cat log.txt |
  grep -P 'rows? in set|^ERROR' |
  sed -r -e 's/^ERROR.*$/null/; s/^.*?\((([0-9.]+) min )?([0-9.]+) sec\).*?$/\2 \3/' |
  awk '{ if ($2) { print $1 * 60 + $2 } else { print $1 } }' |
  awk '{ if (i % 3 == 0) { printf "[" }; printf $1; if (i % 3 != 2) { printf "," } else { print "]," }; ++i; }'
```
