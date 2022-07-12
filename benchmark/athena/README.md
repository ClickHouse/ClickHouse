Data Sources -> AWS Data Catalog -> Create Table -> Use S3 bucket data

Note: Athena does not support files. Only directories.

Go to query editor and run `create.sql`.

```
sudo apt-get install -y jq
export OUTPUT='s3://athena-experiments-milovidov/'

./run1.sh | tee ids.txt
```

Wait a few minutes. Then:

```
cat ids.txt | xargs -I{} aws --output json athena get-query-execution --query-execution-id {} | tee log.txt

cat log.txt | grep -P 'TotalExecutionTimeInMillis|FAILED' | grep -oP '\d+|FAILED' | 
    awk '{ if ($1 == "ERROR") { skip = 1 } else { if (i % 3 == 0) { printf "[" }; printf skip ? "null" : ($1 / 1000); if (i % 3 != 2) { printf "," } else { print "]," }; ++i; skip = 0; } }' 
```
