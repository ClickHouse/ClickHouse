#!/usr/bin/env bash


_main() {
    local dict_filename="${1}"
    if [[ $# -ne 1 ]]; 
    then 
        echo "Usage: $0 <dict_filename>"; 
        exit 1;
    fi

    if [[ ! -f $dict_filename ]];
    then
        echo "File $dict_filename doesn't exist";
        exit 1
    fi

    cat clickhouse-template.g > clickhouse.g
    
    while read line;
    do
        [[ -z "$line" ]] && continue
        echo $line | sed -e '/^#/d' -e 's/"\(.*\)"/" \1 ";/g'
    done < $dict_filename >> clickhouse.g
}

_main "$@"

# Sample run: ./update.sh ${CLICKHOUSE_SOURCE_DIR}/tests/fuzz/all.dict
# then run `python ./gen.py clickhouse.g out.cpp out.proto` to generate new files with tokens. Rebuild fuzzer
