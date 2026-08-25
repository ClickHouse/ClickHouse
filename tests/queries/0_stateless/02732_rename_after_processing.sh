#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Prepare data
unique_name=${CLICKHOUSE_TEST_UNIQUE_NAME}
tmp_dir=${USER_FILES_PATH}/${unique_name}
mkdir -p $tmp_dir
rm -rf ${tmp_dir:?}/*

chmod 777 ${tmp_dir}

echo '"id","str","int","text"' > ${tmp_dir}/tmp.csv
echo '1,"abc",123,"abacaba"' >> ${tmp_dir}/tmp.csv
echo '2,"def",456,"bacabaa"' >> ${tmp_dir}/tmp.csv
echo '3,"story",78912,"acabaab"' >> ${tmp_dir}/tmp.csv
echo '4,"history",21321321,"cabaaba"' >> ${tmp_dir}/tmp.csv

chmod 777 ${tmp_dir}/tmp.csv

cp ${tmp_dir}/tmp.csv ${tmp_dir}/tmp1.csv
cp ${tmp_dir}/tmp.csv ${tmp_dir}/tmp2.csv
cp ${tmp_dir}/tmp.csv ${tmp_dir}/tmp3_1.csv
cp ${tmp_dir}/tmp.csv ${tmp_dir}/tmp3_2.csv
cp ${tmp_dir}/tmp.csv ${tmp_dir}/tmp4.csv
cp ${tmp_dir}/tmp.csv ${tmp_dir}/tmp5.csv
cp ${tmp_dir}/tmp.csv ${tmp_dir}/tmp6.csv

### Checking that renaming works

# simple select
${CLICKHOUSE_CLIENT} --rename-files-after-processing="processed_%f%e" -q "SELECT COUNT(*) FROM file('${unique_name}/tmp1.csv')"
if [ -e "${tmp_dir}/processed_tmp1.csv" ]; then
  echo "processed_tmp1.csv"
fi
if [ ! -e "${tmp_dir}/tmp1.csv" ]; then
  echo "!tmp1.csv"
fi

# select with multiple file() calls
${CLICKHOUSE_CLIENT} --rename-files-after-processing="processed_%f%e" --multiline -q  """
SELECT
    sum(a.id) as aid,
    sum(b.id) as bid
FROM file('${unique_name}/tmp2.csv') AS a
INNER JOIN file('${unique_name}/tmp2.csv') AS b
ON a.text = b.text
"""
if [ -e "${tmp_dir}/processed_tmp2.csv" ]; then
  echo "processed_tmp2.csv"
fi
if [ ! -e "${tmp_dir}/tmp2.csv" ]; then
    echo "!tmp2.csv"
fi

# rename multiple files
${CLICKHOUSE_CLIENT} --rename-files-after-processing="processed_%f%e" -q "SELECT COUNT(*) FROM file('${unique_name}/tmp3*.csv')"
if [ -e "${tmp_dir}/processed_tmp3_1.csv" ]; then
    echo "processed_tmp3_1.csv"
fi
if [ -e "${tmp_dir}/processed_tmp3_2.csv" ]; then
    echo "processed_tmp3_2.csv"
fi
if [ ! -e "${tmp_dir}/tmp3_1.csv" ]; then
    echo "!tmp3_1.csv"
fi
if [ ! -e "${tmp_dir}/tmp3_2.csv" ]; then
    echo "!tmp3_2.csv"
fi

# check timestamp placeholder
${CLICKHOUSE_CLIENT} --rename-files-after-processing="processed_%f_%t.csv" -q "SELECT COUNT(*) FROM file('${unique_name}/tmp4.csv')"
# ls ${tmp_dir} | grep -E "^processed_tmp4_[0-9]+\.csv$" > /dev/null && echo "OK"
rg="processed_tmp4_[0-9]+\.csv"
for x in "${tmp_dir}"/processed*; do
    if [[ $x =~ $rg ]]; then
        echo "OK"
        break
    fi;
done

### Checking errors

# cannot overwrite an existing file
${CLICKHOUSE_CLIENT} --rename-files-after-processing="tmp.csv" -q "SELECT COUNT(*) FROM file('${unique_name}/tmp5.csv')" \
    2>&1| grep "already exists" > /dev/null && echo "OK"
if [ -e "${tmp_dir}/tmp5.csv" ]; then
    echo "tmp5.csv"
fi

# сannot move file outside user_files
${CLICKHOUSE_CLIENT} --rename-files-after-processing="../../%f%e" -q "SELECT COUNT(*) FROM file('${unique_name}/tmp5.csv')" \
    2>&1| grep "is not inside" > /dev/null && echo "OK"
if [ -e "${tmp_dir}/tmp5.csv" ]; then
    echo "tmp5.csv"
fi

# check invalid placeholders

# unknown type of placeholder (%k)
${CLICKHOUSE_CLIENT} --rename-files-after-processing="processed_%f_%k" -q "SELECT COUNT(*) FROM file('${unique_name}/tmp5.csv')" \
    2>&1| grep "Allowed placeholders only" > /dev/null && echo "OK"
if [ -e "${tmp_dir}/tmp5.csv" ]; then
    echo "tmp5.csv"
fi

# dd number of consecutive percentage signs after replace valid placeholders
${CLICKHOUSE_CLIENT} --rename-files-after-processing="processed_%f_%%%%e" -q "SELECT COUNT(*) FROM file('${unique_name}/tmp5.csv')" \
    2>&1| grep "Odd number of consecutive percentage signs" > /dev/null && echo "OK"
if [ -e "${tmp_dir}/tmp5.csv" ]; then
    echo "tmp5.csv"
fi

# check full file name placeholder
${CLICKHOUSE_CLIENT} --rename-files-after-processing="%a.processed" -q "SELECT COUNT(*) FROM file('${unique_name}/tmp6.csv')"
if [ -e "${tmp_dir}/tmp6.csv.processed" ]; then
  echo "tmp6.csv.processed"
fi
if [ ! -e "${tmp_dir}/tmp6.csv" ]; then
    echo "!tmp6.csv"
fi

# Clean
rm -rd $tmp_dir
