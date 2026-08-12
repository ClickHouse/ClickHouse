#!/usr/bin/env bash
# Tags: no-fasttest
# shellcheck disable=SC2155

set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


urls=(
    "http://www.example.com"
    "https://secure.example.com"
    "http://example.com"
    "https://www.example.org"
    "https://subdomain.example.com"
    "http://sub.sub.example.com"
    "http://192.168.1.1"
    "https://[2001:db8:85a3:8d3:1319:8a2e:370:7348]"
    "http://example.com:8080"
    "https://example.com:443"
    "http://example.com/path/to/page.html"
    "https://example.com/path/with/trailing/slash/"
    "http://example.com/search?q=query&lang=en"
    "https://example.com/path?param1=value1&param2=value2"
    "http://example.com/page.html#section1"
    "https://example.com/document.pdf#page=10"
    "http://user:password@example.com"
    "https://user@example.com"
    "https://user:pass@sub.example.com:8080/path/page.html?query=123#fragment"
    "http://example.com/path%20with%20spaces"
    "https://example.com/search?q=encode+this"
    "http://例子.测试"
    "https://mañana.com"
    "http://example.com/%E2%82%AC"
    "data:text/plain;base64,SGVsbG8sIFdvcmxkIQ=="
    "file:///C:/path/to/file.txt"
    "file:///home/user/document.pdf"
    "ftp://ftp.example.com/pub/file.zip"
    "ftps://secure-ftp.example.com/private/doc.pdf"
    "mailto:user@example.com"
    "mailto:user@example.com?subject=Hello&body=How%20are%20you"
    "git://github.com/user/repo.git"
    "ssh://user@host.xz:port/path/to/repo.git"
    "https://example.com/path(1)/[2]/{3}"
    "http://example.com/path;param?query,value"
    ""
    "http://"
    "example.com"
    "http:"
    "//"
    "?query=value"
    "#fragment"
    "http://?#"
    "http://xn--bcher-kva.ch"
    "https://xn--bcher-kva.xn--tckwe/xn--8ws00zhy3a/%E6%B8%AC%E8%A9%A6.php?xn--o39an51a5phao35a=xn--mgbh0fb&xn--fiq228c5hs=test"
    "https://xn--3e0b707e.xn--79-8kcre8v3a/%ED%85%8C%EC%8A%A4%ED%8A%B8/%ED%8C%8C%EC%9D%BC.jsp?xn--i1b6b1a6a2e=xn--9t4b11yi5a&xn--3e0b707e=xn--80aaa1cbgbm"
    "https://example.com/path?param=value&special=!@#$%^&*()"

    "http://example.com/path/with/~tilde"
    "https://example.com/path/with/\`backtick\`"

    "https://example.com/path?param1=value1&param2=value2&param3=value3#section1#section2"
    "http://example.com/page?q1=v1&q2=v2#frag1#frag2#frag3"

    "https://example.com/☃/snowman"
    "http://example.com/path/⽇本語"
    "https://example.com/ü/ñ/path?q=ç"

    "https://example.com/path/to/very/long/url/that/exceeds/two/hundred/and/fifty/five/characters/lorem/ipsum/dolor/sit/amet/consectetur/adipiscing/elit/sed/do/eiusmod/tempor/incididunt/ut/labore/et/dolore/magna/aliqua/ut/enim/ad/minim/veniam/quis/nostrud/exercitation/ullamco/laboris/nisi/ut/aliquip/ex/ea/commodo/consequat"

    "https://example.com//path///to//file"
    "http://example.com/path?param1=value1&&param2=value2&&&param3=value3"

    "http://example.com/%70%61%74%68?%70%61%72%61%6d=%76%61%6c%75%65#%66%72%61%67%6d%65%6e%74"

    "HtTpS://ExAmPlE.cOm/PaTh"
    "http://EXAMPLE.COM/PATH"

    "http://127.0.0.1:8080/path"
    "https://[::1]/path"
    "http://[2001:0db8:85a3:0000:0000:8a2e:0370:7334]:8080/path"

    "http://example.com:65535/path"
    "https://example.com:0/path"

    "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAACklEQVR4nGMAAQAABQABDQottAAAAABJRU5ErkJggg=="

    "https://user:password@example.com:8080/path?query=value#fragment"
    "ftp://anonymous:password@ftp.example.com/pub/"

    "http://example.com/path%20with%20spaces"
    "https://example.com/search?q=query%20with%20spaces"

    "https://www.mañana.com/path"
    "http://例子.测试/path"
    "https://рм.рф/path"

    "https://user:pass@sub.example.com:8080/p/a/t/h?query=123&key=value#fragid1"

    "jdbc:mysql://localhost:3306/database"
    "market://details?id=com.example.app"
    "tel:+1-816-555-1212"
    "sms:+18165551212"

    "http://[1080:0:0:0:8:800:200C:417A]/index.html"
    "https://[2001:db8::1428:57ab]:8080/path"

    "http://.."
    "http://../"
    "http://??"
    "http://??/"
    "http:///a"
    "http://example.com??"
    "http://example.com??/"
    "foo://example.com:8042/over/there?name=ferret#nose"
    "//example.com/path"
)


base64URLEncode() {
    echo -n "$1" | base64 -w0 | tr '+/' '-_' | tr -d '='
}

# Compute all expected values with the shell upfront (no ClickHouse involved),
# then check everything in one bulk query instead of two queries per URL.
# None of the test URLs contain single quotes or SQL-significant backslashes,
# and base64url output (A-Za-z0-9-_) never contains them either, so plain
# SQL string interpolation is safe here.

values=""
sep=""
for url in "${urls[@]}"; do
    expected=$(base64URLEncode "$url")
    url_sql="${url//\'/\'\'}"  # escape ' → '' (defensive, none expected)
    values+="${sep}('${url_sql}','${expected}')"
    sep=","
done

# One query: check that base64URLEncode matches the shell ground truth
# and that base64URLDecode is a left inverse (roundtrip).
# Prints nothing on success; prints failing rows on mismatch.
${CLICKHOUSE_CLIENT} --query="
SELECT
    url,
    base64URLEncode(url) AS ch_encoded,
    expected_encoded,
    base64URLDecode(expected_encoded) AS ch_decoded
FROM VALUES('url String, expected_encoded String', ${values})
WHERE ch_encoded != expected_encoded OR ch_decoded != url
FORMAT TSV"

# special case for '
decode=$(${CLICKHOUSE_CLIENT} --query="SELECT base64URLDecode(base64URLEncode('http://example.com/!$&\'()*+,;=:@/path'))")
if [ "$decode" != "http://example.com/!$&\'()*+,;=:@/path" ]; then
    echo "Special case fail"
    echo "Got:      $decode"
fi
