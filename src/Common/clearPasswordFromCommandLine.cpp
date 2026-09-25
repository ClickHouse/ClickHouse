#include <cstring>
#include <string_view>
#include <vector>
#include <Common/clearPasswordFromCommandLine.h>

using namespace std::literals;

/// Structure, that stores query function name, that requires any secret, and positions,
/// where those secrets are passed

struct SecretRule
{
    std::string_view func;
    std::vector<size_t> secret_pos;
};

static const SecretRule rules[] = {
    {"s3"sv, {1, 2}},
    {"s3Cluster"sv, {2, 3}},
    {"remote"sv, {4}},
    {"remoteSecure"sv, {4}},
    {"mysql"sv, {4}},
    {"postgresql"sv, {4}},
    {"mongodb"sv, {4}},
};

bool isIdentChar(char c)
{
    return std::isalnum(static_cast<unsigned char>(c)) || c == '_';
}

char * mask(char * begin, char * end)
{
    std::memmove(begin, end, std::strlen(end) + 1);
    return begin;
}

void shrederSecretInQuery(char * query)
{
    char * cur = query;
    const std::size_t original_len = std::strlen(query);

    while (*cur)
    {
        while (std::isspace(static_cast<unsigned char>(*cur)))
            ++cur;
        char * id_begin = cur;
        if (!isIdentChar(*cur))
        {
            ++cur;
            continue;
        }
        while (isIdentChar(*cur))
            ++cur;
        std::string_view ident{id_begin, static_cast<size_t>(cur - id_begin)};

        std::vector<size_t> secret;
        for (const auto & r : rules)
            if (ident == r.func)
            {
                secret = r.secret_pos;
                break;
            }

        if (secret.empty())
            continue;

        while (std::isspace(static_cast<unsigned char>(*cur)))
            ++cur;

        if (*cur != '(')
            continue;
        ++cur;

        bool in_quote = false;
        size_t arg_idx = 0;
        char * quote_beg = nullptr;
        size_t secret_index = 0;

        for (; *cur; ++cur)
        {
            char ch = *cur;

            if (!in_quote)
            {
                if (ch == '\'')
                {
                    in_quote = true;
                    quote_beg = cur + 1;
                }
                else if (ch == ',')
                {
                    ++arg_idx;
                }
                else if (ch == ')')
                {
                    break;
                }
            }
            else
            {
                if (ch == '\'' && cur[-1] != '\\')
                {
                    in_quote = false;
                    if (secret[secret_index] == arg_idx)
                    {
                        cur = mask(quote_beg, cur);
                        ++secret_index;
                        if (secret_index > secret.size() - 1)
                            break;
                    }
                }
            }
        }
    }
    std::size_t new_len = std::strlen(query);
    if (new_len < original_len)
        std::memset(query + new_len, 0, original_len - new_len);
}


void clearPasswordFromCommandLine(int argc, char ** argv)
{
    for (int arg = 1; arg < argc; ++arg)
    {
        if (arg + 1 < argc && argv[arg] == "--password"sv)
        {
            ++arg;
            memset(argv[arg], 0, strlen(argv[arg]));
        }
        else if (0 == strncmp(argv[arg], "--password=", strlen("--password=")))
        {
            memset(argv[arg] + strlen("--password="), 0, strlen(argv[arg]) - strlen("--password="));
        }
        else if ((argv[arg] == "--query"sv || argv[arg] == "-q"sv))
        {
            ++arg;
            shrederSecretInQuery(argv[arg]);
        }
        else if (0 == std::strncmp(argv[arg], "--query=", strlen("--query=")))
        {
            shrederSecretInQuery(argv[arg] + strlen("--query="));
        }
    }
}
