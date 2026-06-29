#include <string.h>
#include <stdio.h>
#include <clickhouse_lexer.h>

int main(int argc, char ** argv)
{
    (void)argc;
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

