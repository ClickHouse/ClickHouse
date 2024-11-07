#define _POSIX_C_SOURCE 200112L
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <errno.h>
#include "pdjson.h"

#define JSON_FLAG_ERROR      (1u << 0)
#define JSON_FLAG_STREAMING  (1u << 1)


// patched for poco 1.8.x (VS 2008)
#if defined(_MSC_VER) && (_MSC_VER < 1900)

#define json_error(json, format, ...)                             \
    if (!(json->flags & JSON_FLAG_ERROR)) {                       \
        json->flags |= JSON_FLAG_ERROR;                           \
        _snprintf_s(json->errmsg, sizeof(json->errmsg), _TRUNCATE,\
                 "error: %lu: " format,                           \
                 (unsigned long) json->lineno,                    \
                 __VA_ARGS__);                                    \
    }                                                             \

#else

#define json_error(json, format, ...)                             \
    if (!(json->flags & JSON_FLAG_ERROR)) {                       \
        json->flags |= JSON_FLAG_ERROR;                           \
        snprintf(json->errmsg, sizeof(json->errmsg),              \
                 "error: %lu: " format,                           \
                 (unsigned long) json->lineno,                    \
                 __VA_ARGS__);                                    \
    }                                                             \

#endif // _MSC_VER

#define STACK_INC 4

#if defined(_MSC_VER) || defined(__MINGW32__)
#define strerror_r(err, buf, len) strerror_s(buf, len, err)
#endif
/*
const char *json_typename[] = {
    [JSON_ERROR]      = "ERROR",
    [JSON_DONE]       = "DONE",
    [JSON_OBJECT]     = "OBJECT",
    [JSON_OBJECT_END] = "OBJECT_END",
    [JSON_ARRAY]      = "ARRAY",
    [JSON_ARRAY_END]  = "ARRAY_END",
    [JSON_STRING]     = "STRING",
    [JSON_NUMBER]     = "NUMBER",
    [JSON_TRUE]       = "TRUE",
    [JSON_FALSE]      = "FALSE",
    [JSON_NULL]       = "NULL",
};
*/
struct json_stack {
    enum json_type type;
    long count;
};

static void json_error_s(json_stream *json, int err)
{
    char errbuf[1024] = {0};
    strerror_r(err, errbuf, sizeof(errbuf));
    json_error(json, "%s", errbuf);
}

static enum json_type
push(json_stream *json, enum json_type type)
{
    json->stack_top++;

    if (json->stack_top >= json->stack_size) {
        struct json_stack *stack;
        stack = (struct json_stack *) json->alloc.realloc(json->stack,
                (json->stack_size + STACK_INC) * sizeof(*json->stack));
        if (stack == NULL) {
            json_error_s(json, errno);
            return JSON_ERROR;
        }

        json->stack_size += STACK_INC;
        json->stack = stack;
    }

    json->stack[json->stack_top].type = type;
    json->stack[json->stack_top].count = 0;

    return type;
}

static enum json_type
pop(json_stream *json, int c, enum json_type expected)
{
    if (json->stack == NULL || json->stack[json->stack_top].type != expected) {
        json_error(json, "unexpected byte, '%c'", c);
        return JSON_ERROR;
    }
    json->stack_top--;
    return expected == JSON_ARRAY ? JSON_ARRAY_END : JSON_OBJECT_END;
}

static int buffer_peek(struct json_source *source)
{
    if (source->position < source->source.buffer.length)
        return source->source.buffer.buffer[source->position];
    else
        return EOF;
}

static int buffer_get(struct json_source *source)
{
    int c = source->peek(source);
    source->position++;
    return c;
}

static int stream_get(struct json_source *source)
{
    source->position++;
    return fgetc(source->source.stream.stream);
}

static int stream_peek(struct json_source *source)
{
    int c = fgetc(source->source.stream.stream);
    ungetc(c, source->source.stream.stream);
    return c;
}

static void init(json_stream *json)
{
    json->lineno = 1;
    json->flags = JSON_FLAG_STREAMING;
    json->errmsg[0] = '\0';
    json->ntokens = 0;
    json->next = (enum json_type) 0;

    json->stack = NULL;
    json->stack_top = -1;
    json->stack_size = 0;

    json->data.string = NULL;
    json->data.string_size = 0;
    json->data.string_fill = 0;
    json->source.position = 0;

    json->alloc.malloc = malloc;
    json->alloc.realloc = realloc;
    json->alloc.free = free;
}

static enum json_type
is_match(json_stream *json, const char *pattern, enum json_type type)
{
    for (const char *p = pattern; *p; p++)
        if (*p != json->source.get(&json->source))
            return JSON_ERROR;
    return type;
}

static int pushchar(json_stream *json, int c)
{
    if (json->data.string_fill == json->data.string_size) {
        size_t size = json->data.string_size * 2;
        char *buffer = (char*) json->alloc.realloc(json->data.string, size);
        if (buffer == NULL) {
            json_error_s(json, errno);
            return -1;
        } else {
            json->data.string_size = size;
            json->data.string = buffer;
        }
    }
    json->data.string[json->data.string_fill++] = c;
    return 0;
}

static int init_string(json_stream *json)
{
    json->data.string_fill = 0;
    if (json->data.string == NULL) {
        json->data.string_size = 1024;
        json->data.string = (char*) json->alloc.malloc(json->data.string_size);
        if (json->data.string == NULL) {
            json_error_s(json, errno);
            return -1;
        }
    }
    json->data.string[0] = '\0';
    return 0;
}

static int encode_utf8(json_stream *json, unsigned long c)
{
    if (c < 0x80UL) {
        return pushchar(json, c);
    } else if (c < 0x0800UL) {
        return !((pushchar(json, (c >> 6 & 0x1F) | 0xC0) == 0) &&
                 (pushchar(json, (c >> 0 & 0x3F) | 0x80) == 0));
    } else if (c < 0x010000UL) {
        if (c >= 0xd800 && c <= 0xdfff) {
            json_error(json, "invalid codepoint %06lx", c);
            return -1;
        }
        return !((pushchar(json, (c >> 12 & 0x0F) | 0xE0) == 0) &&
                 (pushchar(json, (c >>  6 & 0x3F) | 0x80) == 0) &&
                 (pushchar(json, (c >>  0 & 0x3F) | 0x80) == 0));
    } else if (c < 0x110000UL) {
        return !((pushchar(json, (c >> 18 & 0x07) | 0xF0) == 0) &&
                (pushchar(json, (c >> 12 & 0x3F) | 0x80) == 0) &&
                (pushchar(json, (c >> 6  & 0x3F) | 0x80) == 0) &&
                (pushchar(json, (c >> 0  & 0x3F) | 0x80) == 0));
    } else {
        json_error(json, "can't encode UTF-8 for %06lx", c);
        return -1;
    }
}

static int hexchar(int c)
{
    switch (c) {
    case '0': return 0;
    case '1': return 1;
    case '2': return 2;
    case '3': return 3;
    case '4': return 4;
    case '5': return 5;
    case '6': return 6;
    case '7': return 7;
    case '8': return 8;
    case '9': return 9;
    case 'a':
    case 'A': return 10;
    case 'b':
    case 'B': return 11;
    case 'c':
    case 'C': return 12;
    case 'd':
    case 'D': return 13;
    case 'e':
    case 'E': return 14;
    case 'f':
    case 'F': return 15;
    default:
        return -1;
    }
}

static long
read_unicode_cp(json_stream *json)
{
    long cp = 0;
    int shift = 12;

    for (size_t i = 0; i < 4; i++) {
        int c = json->source.get(&json->source);
        int hc;

        if (c == EOF) {
            json_error(json, "%s", "unterminated string literal in unicode");
            return -1;
        } else if ((hc = hexchar(c)) == -1) {
            json_error(json, "bad escape unicode byte, '%c'", c);
            return -1;
        }

        cp += hc * (1 << shift);
        shift -= 4;
    }


    return cp;
}

static int read_unicode(json_stream *json)
{
    long cp, h, l;

    if ((cp = read_unicode_cp(json)) == -1) {
        return -1;
    }

    if (cp >= 0xd800 && cp <= 0xdbff) {
        /* This is the high portion of a surrogate pair; we need to read the
         * lower portion to get the codepoint
         */
        h = cp;

        int c = json->source.get(&json->source);
        if (c == EOF) {
            json_error(json, "%s", "unterminated string literal in unicode");
            return -1;
        } else if (c != '\\') {
            json_error(json, "invalid continuation for surrogate pair: '%c', "
                             "expected '\\'", c);
            return -1;
        }

        c = json->source.get(&json->source);
        if (c == EOF) {
            json_error(json, "%s", "unterminated string literal in unicode");
            return -1;
        } else if (c != 'u') {
            json_error(json, "invalid continuation for surrogate pair: '%c', "
                             "expected 'u'", c);
            return -1;
        }

        if ((l = read_unicode_cp(json)) == -1) {
            return -1;
        }

        if (l < 0xdc00 || l > 0xdfff) {
            json_error(json, "invalid surrogate pair continuation \\u%04lx out "
                             "of range (dc00-dfff)", (unsigned long)l);
            return -1;
        }

        cp = ((h - 0xd800) * 0x400) + ((l - 0xdc00) + 0x10000);
    } else if (cp >= 0xdc00 && cp <= 0xdfff) {
            json_error(json, "dangling surrogate \\u%04lx", (unsigned long)cp);
            return -1;
    }

    return encode_utf8(json, cp);
}

int read_escaped(json_stream *json)
{
    int c = json->source.get(&json->source);
    if (c == EOF) {
        json_error(json, "%s", "unterminated string literal in escape");
        return -1;
    } else if (c == 'u') {
        if (read_unicode(json) != 0)
            return -1;
    } else {
        switch (c) {
        case '\\':
        case 'b':
        case 'f':
        case 'n':
        case 'r':
        case 't':
        case '/':
        case '"':
            {
                const char *codes = "\\bfnrt/\"";
                char *p = (char*) strchr(codes, c);
                if (pushchar(json, "\\\b\f\n\r\t/\""[p - codes]) != 0)
                    return -1;
            }
            break;
        default:
            json_error(json, "bad escaped byte, '%c'", c);
            return -1;
        }
    }
    return 0;
}

static int
char_needs_escaping(int c)
{
    if ((c >= 0) && (c < 0x20 || c == 0x22 || c == 0x5c)) {
        return 1;
    }

    return 0;
}

static int
utf8_seq_length(char byte)
{
    unsigned char u = (unsigned char) byte;
    if (u < 0x80) return 1;

    if (0x80 <= u && u <= 0xBF)
    {
        // second, third or fourth byte of a multi-byte
        // sequence, i.e. a "continuation byte"
        return 0;
    }
    else if (u == 0xC0 || u == 0xC1)
    {
        // overlong encoding of an ASCII byte
        return 0;
    }
    else if (0xC2 <= u && u <= 0xDF)
    {
        // 2-byte sequence
        return 2;
    }
    else if (0xE0 <= u && u <= 0xEF)
    {
        // 3-byte sequence
        return 3;
    }
    else if (0xF0 <= u && u <= 0xF4)
    {
        // 4-byte sequence
        return 4;
    }
    else
    {
        // u >= 0xF5
        // Restricted (start of 4-, 5- or 6-byte sequence) or invalid UTF-8
        return 0;
    }
}

static int
is_legal_utf8(const unsigned char *bytes, int length)
{
    if (0 == bytes || 0 == length) return 0;

    unsigned char a;
    const unsigned char* srcptr = bytes + length;
    switch (length)
    {
    default:
        return 0;
        // Everything else falls through when true.
    case 4:
        if ((a = (*--srcptr)) < 0x80 || a > 0xBF) return 0;
    case 3:
        if ((a = (*--srcptr)) < 0x80 || a > 0xBF) return 0;
    case 2:
        a = (*--srcptr);
        switch (*bytes)
        {
        case 0xE0:
            if (a < 0xA0 || a > 0xBF) return 0;
            break;
        case 0xED:
            if (a < 0x80 || a > 0x9F) return 0;
            break;
        case 0xF0:
            if (a < 0x90 || a > 0xBF) return 0;
            break;
        case 0xF4:
            if (a < 0x80 || a > 0x8F) return 0;
            break;
        default:
            if (a < 0x80 || a > 0xBF) return 0;
        }
    case 1:
        if (*bytes >= 0x80 && *bytes < 0xC2) return 0;
    }
    return *bytes <= 0xF4;
}

static int
read_utf8(json_stream* json, int next_char)
{
    int count = utf8_seq_length(next_char);
    if (!count)
    {
        json_error(json, "%s", "Bad character.");
        return -1;
    }

    char buffer[4];
    buffer[0] = next_char;
    for (int i = 1; i < count; ++i)
    {
        buffer[i] = json->source.get(&json->source);;
    }

    if (!is_legal_utf8((unsigned char*) buffer, count))
    {
        json_error(json, "%s", "No legal UTF8 found");
        return -1;
    }

    for (int i = 0; i < count; ++i)
    {
        if (pushchar(json, buffer[i]) != 0)
            return -1;
    }
    return 0;
}

static enum json_type
read_string(json_stream *json)
{
    if (init_string(json) != 0)
        return JSON_ERROR;
    while (1) {
        int c = json->source.get(&json->source);
        if (c == EOF) {
            json_error(json, "%s", "unterminated string literal");
            return JSON_ERROR;
        } else if (c == '"') {
            if (pushchar(json, '\0') == 0)
                return JSON_STRING;
            else
                return JSON_ERROR;
        } else if (c == '\\') {
            if (read_escaped(json) != 0)
                return JSON_ERROR;
        } else if ((unsigned) c >= 0x80) {
            if (read_utf8(json, c) != 0)
                return JSON_ERROR;
        } else {
            if (char_needs_escaping(c)) {
                json_error(json, "%s", "unescaped control character in string");
                return JSON_ERROR;
            }

            if (pushchar(json, c) != 0)
                return JSON_ERROR;
        }
    }
    return JSON_ERROR;
}

static int
is_digit(int c)
{
    return c >= 48 /*0*/ && c <= 57 /*9*/;
}

static int
read_digits(json_stream *json)
{
    unsigned nread = 0;
    while (is_digit(json->source.peek(&json->source))) {
        if (pushchar(json, json->source.get(&json->source)) != 0)
            return -1;

        nread++;
    }

    if (nread == 0) {
        return -1;
    }

    return 0;
}

static enum json_type
read_number(json_stream *json, int c)
{
    if (pushchar(json, c) != 0)
        return JSON_ERROR;
    if (c == '-') {
        c = json->source.get(&json->source);
        if (is_digit(c)) {
            return read_number(json, c);
        } else {
            json_error(json, "unexpected byte, '%c'", c);
        }
    } else if (strchr("123456789", c) != NULL) {
        c = json->source.peek(&json->source);
        if (is_digit(c)) {
            if (read_digits(json) != 0)
                return JSON_ERROR;
        }
    }
    /* Up to decimal or exponent has been read. */
    c = json->source.peek(&json->source);
    if (strchr(".eE", c) == NULL) {
        if (pushchar(json, '\0') != 0)
            return JSON_ERROR;
        else
            return JSON_NUMBER;
    }
    if (c == '.') {
        json->source.get(&json->source); // consume .
        if (pushchar(json, c) != 0)
            return JSON_ERROR;
        if (read_digits(json) != 0)
            return JSON_ERROR;
    }
    /* Check for exponent. */
    c = json->source.peek(&json->source);
    if (c == 'e' || c == 'E') {
        json->source.get(&json->source); // consume e/E
        if (pushchar(json, c) != 0)
            return JSON_ERROR;
        c = json->source.peek(&json->source);
        if (c == '+' || c == '-') {
            json->source.get(&json->source); // consume
            if (pushchar(json, c) != 0)
                return JSON_ERROR;
            if (read_digits(json) != 0)
                return JSON_ERROR;
        } else if (is_digit(c)) {
            if (read_digits(json) != 0)
                return JSON_ERROR;
        } else {
            json_error(json, "unexpected byte in number, '%c'", c);
            return JSON_ERROR;
        }
    }
    if (pushchar(json, '\0') != 0)
        return JSON_ERROR;
    else
        return JSON_NUMBER;
}

static int
json_isspace(int c)
{
    switch (c) {
    case 0x09:
    case 0x0a:
    case 0x0d:
    case 0x20:
        return 1;
    }

    return 0;
}

/* Returns the next non-whitespace character in the stream. */
static int next(json_stream *json)
{
   int c;
   while (json_isspace(c = json->source.get(&json->source)))
       if (c == '\n')
           json->lineno++;
   return c;
}

static enum json_type
read_value(json_stream *json, int c)
{
    json->ntokens++;
    switch (c) {
    case EOF:
        json_error(json, "%s", "unexpected end of data");
        return JSON_ERROR;
    case '{':
        return push(json, JSON_OBJECT);
    case '[':
        return push(json, JSON_ARRAY);
    case '"':
        return read_string(json);
    case 'n':
        return is_match(json, "ull", JSON_NULL);
    case 'f':
        return is_match(json, "alse", JSON_FALSE);
    case 't':
        return is_match(json, "rue", JSON_TRUE);
    case '0':
    case '1':
    case '2':
    case '3':
    case '4':
    case '5':
    case '6':
    case '7':
    case '8':
    case '9':
    case '-':
        if (init_string(json) != 0)
            return JSON_ERROR;
        return read_number(json, c);
    default:
        json_error(json, "unexpected byte, '%c'", c);
        return JSON_ERROR;
    }
}

enum json_type json_peek(json_stream *json)
{
    enum json_type next = json_next(json);
    json->next = next;
    return next;
}

enum json_type json_next(json_stream *json)
{
    if (json->flags & JSON_FLAG_ERROR)
        return JSON_ERROR;
    if (json->next != 0) {
        enum json_type next = json->next;
        json->next = (enum json_type) 0;
        return next;
    }
    if (json->ntokens > 0 && json->stack_top == (size_t)-1) {
        int c;

        do {
            c = json->source.peek(&json->source);
            if (json_isspace(c)) {
                c = json->source.get(&json->source);
            }
        } while (json_isspace(c));

        if (!(json->flags & JSON_FLAG_STREAMING) && c != EOF) {
            return JSON_ERROR;
        }

        return JSON_DONE;
    }
    int c = next(json);
    if (json->stack_top == (size_t)-1)
        return read_value(json, c);
    if (json->stack[json->stack_top].type == JSON_ARRAY) {
        if (json->stack[json->stack_top].count == 0) {
            if (c == ']') {
                return pop(json, c, JSON_ARRAY);
            }
            json->stack[json->stack_top].count++;
            return read_value(json, c);
        } else if (c == ',') {
            json->stack[json->stack_top].count++;
            return read_value(json, next(json));
        } else if (c == ']') {
            return pop(json, c, JSON_ARRAY);
        } else {
            json_error(json, "unexpected byte, '%c'", c);
            return JSON_ERROR;
        }
    } else if (json->stack[json->stack_top].type == JSON_OBJECT) {
        if (json->stack[json->stack_top].count == 0) {
            if (c == '}') {
                return pop(json, c, JSON_OBJECT);
            }

            /* No property value pairs yet. */
            enum json_type value = read_value(json, c);
            if (value != JSON_STRING) {
                json_error(json, "%s", "expected property name or '}'");
                return JSON_ERROR;
            } else {
                json->stack[json->stack_top].count++;
                return value;
            }
        } else if ((json->stack[json->stack_top].count % 2) == 0) {
            /* Expecting comma followed by property name. */
            if (c != ',' && c != '}') {
                json_error(json, "%s", "expected ',' or '}'");
                return JSON_ERROR;
            } else if (c == '}') {
                return pop(json, c, JSON_OBJECT);
            } else {
                enum json_type value = read_value(json, next(json));
                if (value != JSON_STRING) {
                    json_error(json, "%s", "expected property name");
                    return JSON_ERROR;
                } else {
                    json->stack[json->stack_top].count++;
                    return value;
                }
            }
        } else if ((json->stack[json->stack_top].count % 2) == 1) {
            /* Expecting colon followed by value. */
            if (c != ':') {
                json_error(json, "%s", "expected ':' after property name");
                return JSON_ERROR;
            } else {
                json->stack[json->stack_top].count++;
                return read_value(json, next(json));
            }
        }
    }
    json_error(json, "%s", "invalid parser state");
    return JSON_ERROR;
}

void json_reset(json_stream *json)
{
    json->stack_top = -1;
    json->ntokens = 0;
    json->flags &= ~JSON_FLAG_ERROR;
    json->errmsg[0] = '\0';
}

const char *json_get_string(json_stream *json, size_t *length)
{
    if (length != NULL)
        *length = json->data.string_fill;
    if (json->data.string == NULL)
        return "";
    else
        return json->data.string;
}

double json_get_number(json_stream *json)
{
    char *p = json->data.string;
    return p == NULL ? 0 : strtod(p, NULL);
}

const char *json_get_error(json_stream *json)
{
    return json->flags & JSON_FLAG_ERROR ? json->errmsg : NULL;
}

size_t json_get_lineno(json_stream *json)
{
    return json->lineno;
}

size_t json_get_position(json_stream *json)
{
    return json->source.position;
}

size_t json_get_depth(json_stream *json)
{
    return json->stack_top + 1;
}

void json_open_buffer(json_stream *json, const void *buffer, size_t size)
{
    init(json);
    json->source.get = buffer_get;
    json->source.peek = buffer_peek;
    json->source.source.buffer.buffer = (char*) buffer;
    json->source.source.buffer.length = size;
}

void json_open_string(json_stream *json, const char *string)
{
    json_open_buffer(json, string, strlen(string));
}

void json_open_stream(json_stream *json, FILE * stream)
{
    init(json);
    json->source.get = stream_get;
    json->source.peek = stream_peek;
    json->source.source.stream.stream = stream;
}

static int user_get(struct json_source *json)
{
    return json->source.user.get(json->source.user.ptr);
}

static int user_peek(struct json_source *json)
{
    return json->source.user.peek(json->source.user.ptr);
}

void json_open_user(json_stream *json, json_user_io get, json_user_io peek, void *user)
{
    init(json);
    json->source.get = user_get;
    json->source.peek = user_peek;
    json->source.source.user.ptr = user;
    json->source.source.user.get = get;
    json->source.source.user.peek = peek;
}

void json_set_allocator(json_stream *json, json_allocator *a)
{
    json->alloc = *a;
}

void json_set_streaming(json_stream *json, bool streaming)
{
    if (streaming)
        json->flags |= JSON_FLAG_STREAMING;
    else
        json->flags &= ~JSON_FLAG_STREAMING;
}

void json_close(json_stream *json)
{
    json->alloc.free(json->stack);
    json->alloc.free(json->data.string);
}
