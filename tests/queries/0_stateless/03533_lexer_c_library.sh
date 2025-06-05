#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CXX} -c -Os -fno-exceptions -fno-rtti -nostdlib -D LEXER_STANDALONE_BUILD "${CUR_DIR}/../../../src/Parsers/Lexer.cpp" -o "${CLICKHOUSE_TMP}/Lexer.o"

> main.c echo '
extern unsigned long clickhouse_lexer_size;
void clickhouse_lexer_create(void * ptr, const char * begin, const char * end, unsigned long max_query_size);
unsigned char clickhouse_lexer_next_token(void * ptr, const char ** out_token_begin, const char ** out_token_end);
int clickhouse_lexer_token_is_significant(unsigned char token);
int clickhouse_lexer_token_is_error(unsigned char token);
int clickhouse_lexer_token_is_end(unsigned char token);

#include <string.h>
#include <stdio.h>

int main(int argc, char ** argv)
{
    char lexer[clickhouse_lexer_size];
    clickhouse_lexer_create(lexer, argv[1], argv[1] + strlen(argv[1]), 1048576);
    while (1)
    {
        const char * token_begin;
        const char * token_end;
        unsigned char token_type = clickhouse_lexer_next_token(lexer, &token_begin, &token_end);
        if (clickhouse_lexer_token_is_end(token_type) || clickhouse_lexer_token_is_error(token_type))
            break;
        printf("%u: %.*s\n", token_type, (int)(token_end - token_begin), token_begin);
    }

    return 0;
}
'

${CC} main.c "${CLICKHOUSE_TMP}/Lexer.o" -o "${CLICKHOUSE_TMP}/lexer"

"${CLICKHOUSE_TMP}/lexer" 'SELECT 1, 2, /* Hello */ "test" AS x'
