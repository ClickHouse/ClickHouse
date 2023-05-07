#!/usr/bin/env bash
# Tags: no-parallel

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# see 01658_read_file_to_stringcolumn.sh
CLICKHOUSE_USER_FILES_PATH=$(clickhouse-client --query "select _path, _file from file('nonexist.txt', 'CSV', 'val1 char')" 2>&1 | grep Exception | awk '{gsub("/nonexist.txt","",$9); print $9}')

# Prepare data
mkdir -p ${CLICKHOUSE_USER_FILES_PATH}
echo '"id","str","int","text"' > ${CLICKHOUSE_USER_FILES_PATH}/tmp.csv
echo '1,"abc",123,"abacaba"' >> ${CLICKHOUSE_USER_FILES_PATH}/tmp.csv
echo '2,"def",456,"bacabaa"' >> ${CLICKHOUSE_USER_FILES_PATH}/tmp.csv
echo '3,"story",78912,"acabaab"' >> ${CLICKHOUSE_USER_FILES_PATH}/tmp.csv
echo '4,"history",21321321,"cabaaba"' >> ${CLICKHOUSE_USER_FILES_PATH}/tmp.csv

cp ${CLICKHOUSE_USER_FILES_PATH}/tmp.csv ${CLICKHOUSE_USER_FILES_PATH}/tmp1.csv
cp ${CLICKHOUSE_USER_FILES_PATH}/tmp.csv ${CLICKHOUSE_USER_FILES_PATH}/tmp2.csv
cp ${CLICKHOUSE_USER_FILES_PATH}/tmp.csv ${CLICKHOUSE_USER_FILES_PATH}/tmp3_1.csv
cp ${CLICKHOUSE_USER_FILES_PATH}/tmp.csv ${CLICKHOUSE_USER_FILES_PATH}/tmp3_2.csv
cp ${CLICKHOUSE_USER_FILES_PATH}/tmp.csv ${CLICKHOUSE_USER_FILES_PATH}/tmp4.csv
cp ${CLICKHOUSE_USER_FILES_PATH}/tmp.csv ${CLICKHOUSE_USER_FILES_PATH}/tmp5.csv

### Checking that renaming works

# simple select
${CLICKHOUSE_CLIENT} --rename-files-after-processing="processed_%f%e" -q "SELECT COUNT(*) FROM file('tmp1.csv')"
if [ -e "${CLICKHOUSE_USER_FILES_PATH}/processed_tmp1.csv" ]; then
  echo "processed_tmp1.csv"
fi
if [ ! -e "${CLICKHOUSE_USER_FILES_PATH}/tmp1.csv" ]; then
  echo "!tmp1.csv"
fi

# select with multiple file() calls
${CLICKHOUSE_CLIENT} --rename-files-after-processing="processed_%f%e" --multiline -q  """
SELECT
    sum(a.id) as aid,
    sum(b.id) as bid
FROM file('tmp2.csv') AS a
INNER JOIN file('tmp2.csv') AS b
ON a.text = b.text
"""
if [ -e "${CLICKHOUSE_USER_FILES_PATH}/processed_tmp2.csv" ]; then
  echo "processed_tmp2.csv"
fi
if [ ! -e "${CLICKHOUSE_USER_FILES_PATH}/tmp2.csv" ]; then
    echo "!tmp2.csv"
fi

# rename multiple files
${CLICKHOUSE_CLIENT} --rename-files-after-processing="processed_%f%e" -q "SELECT COUNT(*) FROM file('tmp3*.csv')"
if [ -e "${CLICKHOUSE_USER_FILES_PATH}/processed_tmp3_1.csv" ]; then
    echo "processed_tmp3_1.csv"
fi
if [ -e "${CLICKHOUSE_USER_FILES_PATH}/processed_tmp3_2.csv" ]; then
    echo "processed_tmp3_2.csv"
fi
if [ ! -e "${CLICKHOUSE_USER_FILES_PATH}/tmp3_1.csv" ]; then
    echo "!tmp3_1.csv"
fi
if [ ! -e "${CLICKHOUSE_USER_FILES_PATH}/tmp3_2.csv" ]; then
    echo "!tmp3_2.csv"
fi

# check timestamp placeholder
${CLICKHOUSE_CLIENT} --rename-files-after-processing="processed_%f_%t.csv" -q "SELECT COUNT(*) FROM file('tmp4.csv')"
# ls ${CLICKHOUSE_USER_FILES_PATH} | grep -E "^processed_tmp4_[0-9]+\.csv$" > /dev/null && echo "OK"
rg="processed_tmp4_[0-9]+\.csv"
for x in "${CLICKHOUSE_USER_FILES_PATH}"/processed*; do
    if [[ $x =~ $rg ]]; then
        echo "OK"
        break
    fi;
done

### Checking errors

# cannot overwrite an existing file
${CLICKHOUSE_CLIENT} --rename-files-after-processing="tmp.csv" -q "SELECT COUNT(*) FROM file('tmp5.csv')" \
    2>&1| grep "already exists" > /dev/null && echo "OK"
if [ -e "${CLICKHOUSE_USER_FILES_PATH}/tmp5.csv" ]; then
    echo "tmp5.csv"
fi

# сannot move file from user_files
${CLICKHOUSE_CLIENT} --rename-files-after-processing="../%f%e" -q "SELECT COUNT(*) FROM file('tmp5.csv')" \
    2>&1| grep "is not inside" > /dev/null && echo "OK"
if [ -e "${CLICKHOUSE_USER_FILES_PATH}/tmp5.csv" ]; then
    echo "tmp5.csv"
fi

# Clean
rm -rd $CLICKHOUSE_USER_FILES_PATH
