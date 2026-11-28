#include "config.h"

#include <Interpreters/Access/InterpreterCreateTokenQuery.h>

#include <Access/AuthenticationData.h>
#include <Access/Common/AuthenticationType.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsDateTime.h>
#include <Common/Exception.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeString.h>
#include <Formats/FormatFactory.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Access/InterpreterCreateUserQuery.h>
#include <Interpreters/Access/getValidUntilFromAST.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterFactory.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier_fwd.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/Access/ASTAuthenticationData.h>
#include <Parsers/Access/ASTCreateTokenQuery.h>
#include <Parsers/Access/ASTCreateUserQuery.h>
#include <Parsers/Access/ASTRolesOrUsersSet.h>
#include <Parsers/Access/ASTSettingsProfileElement.h>
#include <Parsers/Access/ASTUserNameWithHost.h>
#include <Parsers/ASTDatabaseOrNone.h>
#include <Processors/Sources/SourceFromSingleChunk.h>

#include <boost/algorithm/hex.hpp>

#include <algorithm>

#if USE_SSL
#    include <openssl/err.h>
#    include <openssl/rand.h>
#    include <Common/OpenSSLHelpers.h>
#endif


namespace DB
{

namespace Setting
{
    extern const SettingsUInt64 create_token_default_ttl_seconds;
    extern const SettingsString format;
    extern const SettingsString output_format;
}

namespace ErrorCodes
{
    extern const int FORMAT_IS_NOT_SUITABLE_FOR_OUTPUT;
    extern const int LOGICAL_ERROR;
    extern const int OPENSSL_ERROR;
    extern const int SUPPORT_IS_DISABLED;
}

namespace
{
    /// The alphabet of a generated token. Alphanumeric only, so that a token can be pasted into a URL,
    /// a shell command line or a configuration file without any quoting or escaping.
    constexpr std::string_view TOKEN_ALPHABET = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    static_assert(TOKEN_ALPHABET.size() == 62);

    /// 62^32 is about 2^190, far beyond the reach of an online or an offline attack.
    constexpr size_t TOKEN_LENGTH = 32;

    /// The salt of the stored hash, in bytes; rendered as twice as many hexadecimal characters,
    /// the same way `AuthenticationData::fromAST` salts an explicit `sha256_password`.
    constexpr size_t TOKEN_SALT_SIZE = 32;

    void fillWithSecureRandomBytes([[maybe_unused]] uint8_t * buf, [[maybe_unused]] size_t size)
    {
#if USE_SSL
        if (RAND_bytes(buf, static_cast<int>(size)) != 1)
            throw Exception(ErrorCodes::OPENSSL_ERROR, "RAND_bytes failed: {}", getOpenSSLErrors());
#else
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
                        "CREATE TOKEN is not available, because ClickHouse was built without SSL library");
#endif
    }

    String generateToken()
    {
        /// The largest multiple of the alphabet size which fits in a byte. Bytes at or above it are
        /// discarded instead of being folded with a remainder, which would make the first
        /// `256 % 62` characters of the alphabet more probable than the rest.
        constexpr uint8_t largest_unbiased_byte = 256 / TOKEN_ALPHABET.size() * TOKEN_ALPHABET.size();

        String token;
        token.reserve(TOKEN_LENGTH);

        /// Ask for more bytes than characters, so that the expected number of rounds is one.
        std::array<uint8_t, TOKEN_LENGTH * 2> buf{};
        while (token.size() < TOKEN_LENGTH)
        {
            fillWithSecureRandomBytes(buf.data(), buf.size());
            for (uint8_t byte : buf)
            {
                if (byte >= largest_unbiased_byte)
                    continue;
                token += TOKEN_ALPHABET[byte % TOKEN_ALPHABET.size()];
                if (token.size() == TOKEN_LENGTH)
                    break;
            }
        }

        return token;
    }

    String generateSalt()
    {
        std::array<uint8_t, TOKEN_SALT_SIZE> key{};
        fillWithSecureRandomBytes(key.data(), key.size());

        String salt;
        salt.resize(key.size() * 2);
        char * buf_pos = salt.data();
        for (uint8_t k : key)
        {
            writeHexByteUppercase(k, buf_pos);
            buf_pos += 2;
        }

        return salt;
    }

    /// Resolves the deadline of the token: the `VALID UNTIL` / `VALID FOR` clause of the query when it
    /// has one, otherwise `create_token_default_ttl_seconds` from now. Returns `0` when the token never
    /// expires, which is what an explicit `VALID UNTIL 'infinity'` and a zero default TTL both mean.
    time_t resolveValidUntil(const ASTCreateTokenQuery & query, ContextPtr context)
    {
        const time_t now = getCurrentTime();

        if (query.valid_until)
            return getValidUntilFromAST(query.valid_until, context, query.valid_until_is_interval, now);

        const UInt64 default_ttl = context->getSettingsRef()[Setting::create_token_default_ttl_seconds];
        if (default_ttl == 0)
            return 0;

        /// The default TTL means exactly `VALID FOR INTERVAL <default_ttl> SECOND`, so it is resolved by
        /// the same call rather than by a second implementation of the same arithmetic: `now` stays
        /// signed (a clock before 1970 yields a deadline that many seconds after that clock, and a
        /// deadline which is still in the past is clamped to the smallest expired instant - fail-closed,
        /// whereas treating a negative `now` as an overflow would saturate every default TTL at year
        /// 9999), and a huge TTL saturates at the largest deadline which displays exactly in every time
        /// zone. The TTL is capped at that same bound before it becomes a literal, because it is a
        /// `UInt64` setting and a value beyond the `Int64` range has no interval representation.
        const auto ttl_seconds = static_cast<Int64>(std::min<UInt64>(default_ttl, static_cast<UInt64>(MAX_VALID_UNTIL_TIME)));
        auto interval = makeASTFunction("toIntervalSecond", make_intrusive<ASTLiteral>(Field(ttl_seconds)));
        return getValidUntilFromAST(interval, context, /*is_interval=*/true, now);
    }

    /// Rejects an output format that cannot be used, before anything has been persisted.
    ///
    /// The result of a query is formatted after its interpreter has returned - by `executeQuery` over
    /// HTTP, and by the client itself over the native protocol - so an unusable `FORMAT` clause would
    /// otherwise be reported only once the authentication method has been added. The user would be left
    /// with a method whose secret nobody knows: it cannot be used, there is no statement which drops a
    /// single authentication method, and it still counts against `max_authentication_methods_per_user`.
    ///
    /// The resolution order is the one of `executeQuery`: the `output_format` and the `format` settings
    /// override the `FORMAT` clause of the query. The server default is deliberately not checked - this
    /// query did not ask for it, and over the native protocol the result is formatted by the client with
    /// its own default, which the server does not know.
    void checkRequestedOutputFormat(const ASTCreateTokenQuery & query, const ContextPtr & context)
    {
        const auto & settings = context->getSettingsRef();

        String format_name = settings[Setting::output_format];
        if (format_name.empty())
            format_name = settings[Setting::format];
        if (format_name.empty() && query.format_ast)
            format_name = getIdentifierName(query.format_ast);
        if (format_name.empty())
            return;

        const auto & format_factory = FormatFactory::instance();
        format_factory.checkFormatName(format_name);
        if (!format_factory.isOutputFormat(format_name))
            throw Exception(ErrorCodes::FORMAT_IS_NOT_SUITABLE_FOR_OUTPUT, "Format {} is not suitable for output", format_name);
    }
}

BlockIO InterpreterCreateTokenQuery::execute()
{
    const auto & query = query_ptr->as<const ASTCreateTokenQuery &>();

    /// A token is always an additional credential of the user who runs the query, so there has to be one.
    /// A context without a user is internal (the access checks are bypassed there), and silently picking
    /// some user to attach a new credential to would be the worst possible guess.
    const String user_name = getContext()->getUserName();
    if (user_name.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "CREATE TOKEN is executed in a context which has no current user");

    /// Before the statement changes anything: a token which cannot be shown to the user must not be
    /// created at all (see `checkRequestedOutputFormat`).
    checkRequestedOutputFormat(query, getContext());

    /// The deadline is resolved here rather than left to the statement below, so that the value
    /// reported back to the user is exactly the one stored on the authentication method: the
    /// clause is rewritten to the resolved absolute instant before the statement runs.
    const time_t valid_until = resolveValidUntil(query, getContext());

    const String token = generateToken();
    const String salt = generateSalt();

    /// Hash the token here and hand the statement below a `sha256_hash` authentication method rather than a
    /// password, for two reasons. The plaintext token then never enters an AST, so it cannot leak through
    /// any of the paths which format one (`ON CLUSTER` distribution, logging of a rewritten query, an
    /// exception message quoting the AST). And the password complexity rules, which the password form would
    /// apply, do not get a say over a server-generated secret: they constrain passwords chosen by humans,
    /// and a rule such as "must contain a punctuation character" would fail every `CREATE TOKEN` with an
    /// error the user cannot act on.
    ///
    /// This matches how `AuthenticationData::fromAST` stores an explicit `sha256_password`: the salt is
    /// appended to the secret and the hash of the concatenation is stored next to the salt.
    const auto digest = AuthenticationData::Util::encodeSHA256(token + salt);
    String hash_hex;
    hash_hex.resize(digest.size() * 2);
    boost::algorithm::hex(digest.begin(), digest.end(), hash_hex.data());

    auto authentication_method = make_intrusive<ASTAuthenticationData>();
    authentication_method->type = AuthenticationType::SHA256_PASSWORD;
    authentication_method->contains_hash = true;
    authentication_method->children.push_back(make_intrusive<ASTLiteral>(hash_hex));
    authentication_method->children.push_back(make_intrusive<ASTLiteral>(salt));
    /// `0` is the "no expiration" sentinel, which is expressed by the absence of the clause.
    if (valid_until != 0)
        authentication_method->setValidUntil(make_intrusive<ASTLiteral>(formatValidUntilInUTC(valid_until)));
    authentication_method->grants = query.grants;

    /// `CREATE TOKEN` is a shorthand for adding an authentication method to the current user, so it is
    /// executed as exactly that statement: the access check (which accepts the `CREATE TOKEN` privilege for
    /// this shape of the statement), the limit on the number of authentication methods per user and the
    /// validation of the `GRANTS` clause all live there and stay in one place.
    auto alter_user_query = make_intrusive<ASTCreateUserQuery>();
    alter_user_query->alter = true;
    alter_user_query->add_identified_with = true;
    alter_user_query->names = make_intrusive<ASTUserNamesWithHost>(user_name);
    alter_user_query->authentication_methods.push_back(authentication_method);
    alter_user_query->children.push_back(authentication_method);

    InterpreterCreateUserQuery{alter_user_query, getContext()}.execute();

    auto token_column = ColumnString::create();
    token_column->insert(token);

    /// `0` means the token never expires, the same encoding as the `valid_until` column of `system.users`.
    auto valid_until_column = ColumnDateTime64::create(0, 0);
    valid_until_column->insertValue(static_cast<Int64>(valid_until));

    BlockIO res;
    res.pipeline = QueryPipeline(std::make_shared<SourceFromSingleChunk>(std::make_shared<const Block>(Block{
        {std::move(token_column), std::make_shared<DataTypeString>(), "token"},
        {std::move(valid_until_column), std::make_shared<DataTypeDateTime64>(0), "valid_until"}})));

    return res;
}

void registerInterpreterCreateTokenQuery(InterpreterFactory & factory);
void registerInterpreterCreateTokenQuery(InterpreterFactory & factory)
{
    auto create_fn = [] (const InterpreterFactory::Arguments & args)
    {
        return std::make_unique<InterpreterCreateTokenQuery>(args.query, args.context);
    };
    factory.registerInterpreter("InterpreterCreateTokenQuery", create_fn);
}

}
