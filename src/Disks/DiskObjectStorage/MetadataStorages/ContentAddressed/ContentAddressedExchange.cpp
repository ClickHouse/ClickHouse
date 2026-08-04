#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/ContentAddressedExchange.h>

#include <array>

namespace DB
{

namespace
{

/// The version tag. A peer that knows a different token shape must be refused rather than
/// misparsed, and a bare field count is not a version: two shapes can agree on it by accident.
constexpr std::string_view kTokenVersion = "car1";
/// `|` is a legal cookie octet (RFC 6265 excludes only CTLs, whitespace, `"`, `,`, `;` and `\`)
/// and never appears in an encoded field, because encoding leaves only the unreserved set and `%`.
constexpr char kFieldSeparator = '|';
constexpr size_t kFieldCount = 6;
/// Sized for the widest real field -- a namespace is `<server_root_id>/store/<u3>/<uuid>@cas@` and a
/// ref name is a part name, possibly `detached/`-prefixed. The cap is a bound on what a peer can make
/// this server hold and log, not a schema: a field that needs more than this is refused, and a refused
/// token costs a byte fetch.
constexpr size_t kMaxFieldBytes = 256;
constexpr size_t kMaxTokenBytes = 1024;

bool isUnreserved(unsigned char c)
{
    return (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9')
        || c == '-' || c == '.' || c == '_' || c == '~';
}

/// Control characters never travel: they would break the cookie header and they are the classic way to
/// smuggle a forged line into whatever log prints the token.
bool isControl(unsigned char c)
{
    return c < 0x20 || c == 0x7F;
}

void appendPercentEncoded(std::string_view field, String & out)
{
    static constexpr std::string_view hex_digits = "0123456789ABCDEF";
    for (const char ch : field)
    {
        const auto c = static_cast<unsigned char>(ch);
        if (isUnreserved(c))
        {
            out += ch;
        }
        else
        {
            out += '%';
            out += hex_digits[c >> 4];
            out += hex_digits[c & 0x0F];
        }
    }
}

std::optional<unsigned char> hexNibble(char ch)
{
    const auto c = static_cast<unsigned char>(ch);
    if (c >= '0' && c <= '9')
        return static_cast<unsigned char>(c - '0');
    if (c >= 'A' && c <= 'F')
        return static_cast<unsigned char>(c - 'A' + 10);
    if (c >= 'a' && c <= 'f')
        return static_cast<unsigned char>(c - 'a' + 10);
    return std::nullopt;
}

/// Strict inverse of `appendPercentEncoded`: a `%` must be followed by exactly two hex digits, and no
/// byte outside the unreserved set may appear unescaped. Lenient decoding is how a percent-encoding
/// pair stops being a bijection, and this one has to be a bijection -- the encoded form is what routes
/// the confirm, the decoded form is what it compares.
std::optional<String> percentDecodeStrict(std::string_view field)
{
    String out;
    out.reserve(field.size());
    for (size_t i = 0; i < field.size(); ++i)
    {
        if (field[i] != '%')
        {
            if (!isUnreserved(static_cast<unsigned char>(field[i])))
                return std::nullopt;
            out += field[i];
            continue;
        }

        if (i + 2 >= field.size())
            return std::nullopt;
        const auto hi = hexNibble(field[i + 1]);
        const auto lo = hexNibble(field[i + 2]);
        if (!hi || !lo)
            return std::nullopt;
        const auto decoded = static_cast<unsigned char>((*hi << 4) | *lo);
        if (isControl(decoded))
            return std::nullopt;
        out += static_cast<char>(decoded);
        i += 2;
    }
    return out;
}

}

std::optional<String> encodeCasRelinkSourceToken(const CasRelinkSourceToken & token)
{
    const std::array<const String *, kFieldCount> fields{
        &token.pool_uuid, &token.server_root_id, &token.root_namespace,
        &token.ref_name, &token.part_name, &token.manifest_ref_text};

    String out{kTokenVersion};
    for (const String * field : fields)
    {
        /// Every field is required: a token with a hole in it can only route somewhere it was not meant
        /// to, and `resolveContentAddressedConfirm` would have to re-discover that for itself.
        if (field->empty() || field->size() > kMaxFieldBytes)
            return std::nullopt;
        for (const char ch : *field)
            if (isControl(static_cast<unsigned char>(ch)))
                return std::nullopt;

        out += kFieldSeparator;
        appendPercentEncoded(*field, out);
    }

    if (out.size() > kMaxTokenBytes)
        return std::nullopt;
    return out;
}

std::optional<CasRelinkSourceToken> decodeCasRelinkSourceToken(std::string_view text)
{
    if (text.size() > kMaxTokenBytes)
        return std::nullopt;

    std::array<std::string_view, kFieldCount + 1> segments;
    size_t count = 0;
    size_t pos = 0;
    while (true)
    {
        if (count == segments.size())
            return std::nullopt;   /// more separators than the shape has fields
        const size_t sep = text.find(kFieldSeparator, pos);
        if (sep == std::string_view::npos)
        {
            segments[count++] = text.substr(pos);
            break;
        }
        segments[count++] = text.substr(pos, sep - pos);
        pos = sep + 1;
    }

    if (count != segments.size() || segments[0] != kTokenVersion)
        return std::nullopt;

    CasRelinkSourceToken token;
    const std::array<String *, kFieldCount> fields{
        &token.pool_uuid, &token.server_root_id, &token.root_namespace,
        &token.ref_name, &token.part_name, &token.manifest_ref_text};

    for (size_t i = 0; i < kFieldCount; ++i)
    {
        auto decoded = percentDecodeStrict(segments[i + 1]);
        if (!decoded || decoded->empty() || decoded->size() > kMaxFieldBytes)
            return std::nullopt;
        *fields[i] = std::move(*decoded);
    }
    return token;
}

}
