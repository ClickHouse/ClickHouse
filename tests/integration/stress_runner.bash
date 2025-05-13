for (( i = 1; i < 10; i++ ))
do
sudo ./runner --binary /home/codestyle/workspace/ClickHouse/build/programs/clickhouse  -- test_aligned_cache_s3 -ss
done
