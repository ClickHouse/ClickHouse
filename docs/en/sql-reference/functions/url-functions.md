---
slug: /en/sql-reference/functions/url-functions
sidebar_position: 200
sidebar_label: URLs
---

# Functions for Working with URLs

:::note
The functions mentioned in this section are optimized for maximum performance and for the most part do not follow the RFC-3986 standard. Functions which implement RFC-3986 have `RFC` appended to their function name and are generally slower.
:::

You can generally use the non-`RFC` function variants when working with publicly registered domains that contain neither user strings nor `@` symbols.
The table below details which symbols in a URL can (`✔`) or cannot (`✗`) be parsed by the respective `RFC` and non-`RFC` variants:

|Symbol | non-`RFC`| `RFC` |
|-------|----------|-------|
| ' '   | ✗        |✗      |
|  \t   | ✗        |✗      |
|  <    | ✗        |✗      |
|  >    | ✗        |✗      |
|  %    | ✗        |✔*     |
|  {    | ✗        |✗      |
|  }    | ✗        |✗      |
|  \|   | ✗        |✗      |
|  \\\  | ✗        |✗      |
|  ^    | ✗        |✗      |
|  ~    | ✗        |✔*     |
|  [    | ✗        |✗      |
|  ]    | ✗        |✔      |
|  ;    | ✗        |✔*     |
|  =    | ✗        |✔*     |
|  &    | ✗        |✔*     |

symbols marked `*` are sub-delimiters in RFC 3986 and allowed for user info following the `@` symbol.

## Functions that Extract Parts of a URL

If the relevant part isn’t present in a URL, an empty string is returned.

### protocol

Extracts the protocol from a URL.

Examples of typical returned values: http, https, ftp, mailto, tel, magnet.

### domain

Extracts the hostname from a URL.

**Syntax**

``` sql
domain(url)
```

**Arguments**

- `url` — URL. [String](../../sql-reference/data-types/string.md).

The URL can be specified with or without a protocol. Examples:

``` text
svn+ssh://some.svn-hosting.com:80/repo/trunk
some.svn-hosting.com:80/repo/trunk
https://clickhouse.com/time/
```

For these examples, the `domain` function returns the following results:

``` text
some.svn-hosting.com
some.svn-hosting.com
clickhouse.com
```

**Returned values**

- Host name if the input string can be parsed as a URL, otherwise an empty string. [String](../data-types/string.md).

**Example**

``` sql
SELECT domain('svn+ssh://some.svn-hosting.com:80/repo/trunk');
```

``` text
┌─domain('svn+ssh://some.svn-hosting.com:80/repo/trunk')─┐
│ some.svn-hosting.com                                   │
└────────────────────────────────────────────────────────┘
```

### domainRFC

Extracts the hostname from a URL. Similar to [domain](#domain), but RFC 3986 conformant.

**Syntax**

``` sql
domainRFC(url)
```

**Arguments**

- `url` — URL. [String](../data-types/string.md).

**Returned values**

- Host name if the input string can be parsed as a URL, otherwise an empty string. [String](../data-types/string.md).

**Example**

``` sql
SELECT
    domain('http://user:password@example.com:8080/path?query=value#fragment'),
    domainRFC('http://user:password@example.com:8080/path?query=value#fragment');
```

``` text
┌─domain('http://user:password@example.com:8080/path?query=value#fragment')─┬─domainRFC('http://user:password@example.com:8080/path?query=value#fragment')─┐
│                                                                           │ example.com                                                                  │
└───────────────────────────────────────────────────────────────────────────┴──────────────────────────────────────────────────────────────────────────────┘
```

### domainWithoutWWW

Returns the domain without leading `www.` if present.

**Syntax**

```sql
domainWithoutWWW(url)
```

**Arguments**

- `url` — URL. [String](../data-types/string.md).

**Returned values**

- Domain name if the input string can be parsed as a URL (without leading `www.`), otherwise an empty string. [String](../data-types/string.md).

**Example**

``` sql
SELECT domainWithoutWWW('http://paul@www.example.com:80/');
```

``` text
┌─domainWithoutWWW('http://paul@www.example.com:80/')─┐
│ example.com                                         │
└─────────────────────────────────────────────────────┘
```

### domainWithoutWWWRFC

Returns the domain without leading `www.` if present. Similar to [domainWithoutWWW](#domainwithoutwww) but conforms to RFC 3986.

**Syntax**

```sql
domainWithoutWWWRFC(url)
```

**Arguments**

- `url` — URL. [String](../data-types/string.md).

**Returned values**

- Domain name if the input string can be parsed as a URL (without leading `www.`), otherwise an empty string. [String](../data-types/string.md).

**Example**

Query:

```sql
SELECT
    domainWithoutWWW('http://user:password@www.example.com:8080/path?query=value#fragment'),
    domainWithoutWWWRFC('http://user:password@www.example.com:8080/path?query=value#fragment');
```

Result:

```response
┌─domainWithoutWWW('http://user:password@www.example.com:8080/path?query=value#fragment')─┬─domainWithoutWWWRFC('http://user:password@www.example.com:8080/path?query=value#fragment')─┐
│                                                                                         │ example.com                                                                                │
└─────────────────────────────────────────────────────────────────────────────────────────┴────────────────────────────────────────────────────────────────────────────────────────────┘
```

### topLevelDomain

Extracts the the top-level domain from a URL.

``` sql
topLevelDomain(url)
```

**Arguments**

- `url` — URL. [String](../../sql-reference/data-types/string.md).

:::note
The URL can be specified with or without a protocol. Examples:

``` text
svn+ssh://some.svn-hosting.com:80/repo/trunk
some.svn-hosting.com:80/repo/trunk
https://clickhouse.com/time/
```
:::

**Returned values**

- Domain name if the input string can be parsed as a URL. Otherwise, an empty string. [String](../../sql-reference/data-types/string.md).

**Example**

Query:

``` sql
SELECT topLevelDomain('svn+ssh://www.some.svn-hosting.com:80/repo/trunk');
```

Result:

``` text
┌─topLevelDomain('svn+ssh://www.some.svn-hosting.com:80/repo/trunk')─┐
│ com                                                                │
└────────────────────────────────────────────────────────────────────┘
```

### topLevelDomainRFC

Extracts the the top-level domain from a URL.
Similar to [topLevelDomain](#topleveldomain), but conforms to RFC 3986.

``` sql
topLevelDomainRFC(url)
```

**Arguments**

- `url` — URL. [String](../../sql-reference/data-types/string.md).

:::note
The URL can be specified with or without a protocol. Examples:

``` text
svn+ssh://some.svn-hosting.com:80/repo/trunk
some.svn-hosting.com:80/repo/trunk
https://clickhouse.com/time/
```
:::

**Returned values**

- Domain name if the input string can be parsed as a URL. Otherwise, an empty string. [String](../../sql-reference/data-types/string.md).

**Example**

Query:

``` sql
SELECT topLevelDomain('http://foo:foo%41bar@foo.com'), topLevelDomainRFC('http://foo:foo%41bar@foo.com');
```

Result:

``` text
┌─topLevelDomain('http://foo:foo%41bar@foo.com')─┬─topLevelDomainRFC('http://foo:foo%41bar@foo.com')─┐
│                                                │ com                                               │
└────────────────────────────────────────────────┴───────────────────────────────────────────────────┘
```

### firstSignificantSubdomain

Returns the “first significant subdomain”.
The first significant subdomain is a second-level domain for `com`, `net`, `org`, or `co`, otherwise it is a third-level domain.
For example, `firstSignificantSubdomain (‘https://news.clickhouse.com/’) = ‘clickhouse’, firstSignificantSubdomain (‘https://news.clickhouse.com.tr/’) = ‘clickhouse’`.
The list of "insignificant" second-level domains and other implementation details may change in the future.

**Syntax**

```sql
firstSignificantSubdomain(url)
```

**Arguments**

- `url` — URL. [String](../../sql-reference/data-types/string.md).

**Returned value**

- The first significant subdomain. [String](../data-types/string.md).

**Example**

Query:

```sql
SELECT firstSignificantSubdomain('http://www.example.com/a/b/c?a=b')
```

Result:

```reference
┌─firstSignificantSubdomain('http://www.example.com/a/b/c?a=b')─┐
│ example                                                       │
└───────────────────────────────────────────────────────────────┘
```

### firstSignificantSubdomainRFC

Returns the “first significant subdomain”.
The first significant subdomain is a second-level domain for `com`, `net`, `org`, or `co`, otherwise it is a third-level domain.
For example, `firstSignificantSubdomain (‘https://news.clickhouse.com/’) = ‘clickhouse’, firstSignificantSubdomain (‘https://news.clickhouse.com.tr/’) = ‘clickhouse’`.
The list of "insignificant" second-level domains and other implementation details may change in the future.
Similar to [firstSignficantSubdomain](#firstsignificantsubdomain) but conforms to RFC 1034.

**Syntax**

```sql
firstSignificantSubdomainRFC(url)
```

**Arguments**

- `url` — URL. [String](../../sql-reference/data-types/string.md).

**Returned value**

- The first significant subdomain. [String](../data-types/string.md).

**Example**

Query:

```sql
SELECT
    firstSignificantSubdomain('http://user:password@example.com:8080/path?query=value#fragment'),
    firstSignificantSubdomainRFC('http://user:password@example.com:8080/path?query=value#fragment');
```

Result:

```reference
┌─firstSignificantSubdomain('http://user:password@example.com:8080/path?query=value#fragment')─┬─firstSignificantSubdomainRFC('http://user:password@example.com:8080/path?query=value#fragment')─┐
│                                                                                              │ example                                                                                         │
└──────────────────────────────────────────────────────────────────────────────────────────────┴─────────────────────────────────────────────────────────────────────────────────────────────────┘
```

### cutToFirstSignificantSubdomain

Returns the part of the domain that includes top-level subdomains up to the [“first significant subdomain”](#firstsignificantsubdomain).

**Syntax**

```sql
cutToFirstSignificantSubdomain(url)
```

**Arguments**

- `url` — URL. [String](../../sql-reference/data-types/string.md).

**Returned value**

- Part of the domain that includes top-level subdomains up to the first significant subdomain if possible, otherwise returns an empty string. [String](../data-types/string.md).

**Example**

Query:

```sql
SELECT
    cutToFirstSignificantSubdomain('https://news.clickhouse.com.tr/'),
    cutToFirstSignificantSubdomain('www.tr'),
    cutToFirstSignificantSubdomain('tr');
```

Result:

```response
┌─cutToFirstSignificantSubdomain('https://news.clickhouse.com.tr/')─┬─cutToFirstSignificantSubdomain('www.tr')─┬─cutToFirstSignificantSubdomain('tr')─┐
│ clickhouse.com.tr                                                 │ tr                                       │                                      │
└───────────────────────────────────────────────────────────────────┴──────────────────────────────────────────┴──────────────────────────────────────┘
```

### cutToFirstSignificantSubdomainRFC

Returns the part of the domain that includes top-level subdomains up to the [“first significant subdomain”](#firstsignificantsubdomain).
Similar to [cutToFirstSignificantSubdomain](#cuttofirstsignificantsubdomain) but conforms to RFC 3986.

**Syntax**

```sql
cutToFirstSignificantSubdomainRFC(url)
```

**Arguments**

- `url` — URL. [String](../../sql-reference/data-types/string.md).

**Returned value**

- Part of the domain that includes top-level subdomains up to the first significant subdomain if possible, otherwise returns an empty string. [String](../data-types/string.md).

**Example**

Query:

```sql
SELECT
    cutToFirstSignificantSubdomain('http://user:password@example.com:8080'),
    cutToFirstSignificantSubdomainRFC('http://user:password@example.com:8080');
```

Result:

```response
┌─cutToFirstSignificantSubdomain('http://user:password@example.com:8080')─┬─cutToFirstSignificantSubdomainRFC('http://user:password@example.com:8080')─┐
│                                                                         │ example.com                                                                │
└─────────────────────────────────────────────────────────────────────────┴────────────────────────────────────────────────────────────────────────────┘
```


### cutToFirstSignificantSubdomainWithWWW

Returns the part of the domain that includes top-level subdomains up to the "first significant subdomain", without stripping `www`.

**Syntax**

```sql
cutToFirstSignificantSubdomainWithWWW(url)
```

**Arguments**

- `url` — URL. [String](../../sql-reference/data-types/string.md).

**Returned value**

- Part of the domain that includes top-level subdomains up to the first significant subdomain (with `www`) if possible, otherwise returns an empty string. [String](../data-types/string.md).

**Example**

Query:

```sql
SELECT
    cutToFirstSignificantSubdomainWithWWW('https://news.clickhouse.com.tr/'),
    cutToFirstSignificantSubdomainWithWWW('www.tr'),
    cutToFirstSignificantSubdomainWithWWW('tr');
```

Result:

```response
┌─cutToFirstSignificantSubdomainWithWWW('https://news.clickhouse.com.tr/')─┬─cutToFirstSignificantSubdomainWithWWW('www.tr')─┬─cutToFirstSignificantSubdomainWithWWW('tr')─┐
│ clickhouse.com.tr                                                        │ www.tr                                          │                                             │
└──────────────────────────────────────────────────────────────────────────┴─────────────────────────────────────────────────┴─────────────────────────────────────────────┘
```

### cutToFirstSignificantSubdomainWithWWWRFC

Returns the part of the domain that includes top-level subdomains up to the "first significant subdomain", without stripping `www`.
Similar to [cutToFirstSignificantSubdomainWithWWW](#cuttofirstsignificantsubdomaincustomwithwww) but conforms to RFC 3986.

**Syntax**

```sql
cutToFirstSignificantSubdomainWithWWW(url)
```

**Arguments**

- `url` — URL. [String](../../sql-reference/data-types/string.md).

**Returned value**

- Part of the domain that includes top-level subdomains up to the first significant subdomain (with "www") if possible, otherwise returns an empty string. [String](../data-types/string.md).

**Example**

Query:

```sql
SELECT
    cutToFirstSignificantSubdomainWithWWW('http:%2F%2Fwwwww.nova@mail.ru/economicheskiy'),
    cutToFirstSignificantSubdomainWithWWWRFC('http:%2F%2Fwwwww.nova@mail.ru/economicheskiy');
```

Result:

```response
┌─cutToFirstSignificantSubdomainWithWWW('http:%2F%2Fwwwww.nova@mail.ru/economicheskiy')─┬─cutToFirstSignificantSubdomainWithWWWRFC('http:%2F%2Fwwwww.nova@mail.ru/economicheskiy')─┐
│                                                                                       │ mail.ru                                                                                  │
└───────────────────────────────────────────────────────────────────────────────────────┴──────────────────────────────────────────────────────────────────────────────────────────┘
```

### cutToFirstSignificantSubdomainCustom

Returns the part of the domain that includes top-level subdomains up to the first significant subdomain.
Accepts custom [TLD list](https://en.wikipedia.org/wiki/List_of_Internet_top-level_domains) name.
This function can be useful if you need a fresh TLD list or if you have a custom list.

**Configuration example**

```xml
<!-- <top_level_domains_path>/var/lib/clickhouse/top_level_domains/</top_level_domains_path> -->
<top_level_domains_lists>
    <!-- https://publicsuffix.org/list/public_suffix_list.dat -->
    <public_suffix_list>public_suffix_list.dat</public_suffix_list>
    <!-- NOTE: path is under top_level_domains_path -->
</top_level_domains_lists>
```

**Syntax**

``` sql
cutToFirstSignificantSubdomain(url, tld)
```

**Arguments**

- `url` — URL. [String](../../sql-reference/data-types/string.md).
- `tld` — Custom TLD list name. [String](../../sql-reference/data-types/string.md).

**Returned value**

- Part of the domain that includes top-level subdomains up to the first significant subdomain. [String](../../sql-reference/data-types/string.md).

**Example**

Query:

```sql
SELECT cutToFirstSignificantSubdomainCustom('bar.foo.there-is-no-such-domain', 'public_suffix_list');
```

Result:

```text
┌─cutToFirstSignificantSubdomainCustom('bar.foo.there-is-no-such-domain', 'public_suffix_list')─┐
│ foo.there-is-no-such-domain                                                                   │
└───────────────────────────────────────────────────────────────────────────────────────────────┘
```

**See Also**

- [firstSignificantSubdomain](#firstsignificantsubdomain).

### cutToFirstSignificantSubdomainCustomRFC

Returns the part of the domain that includes top-level subdomains up to the first significant subdomain.
Accepts custom [TLD list](https://en.wikipedia.org/wiki/List_of_Internet_top-level_domains) name.
This function can be useful if you need a fresh TLD list or if you have a custom list.
Similar to [cutToFirstSignificantSubdomainCustom](#cuttofirstsignificantsubdomaincustom) but conforms to RFC 3986.

**Syntax**

``` sql
cutToFirstSignificantSubdomainRFC(url, tld)
```

**Arguments**

- `url` — URL. [String](../../sql-reference/data-types/string.md).
- `tld` — Custom TLD list name. [String](../../sql-reference/data-types/string.md).

**Returned value**

- Part of the domain that includes top-level subdomains up to the first significant subdomain. [String](../../sql-reference/data-types/string.md).

**See Also**

- [firstSignificantSubdomain](#firstsignificantsubdomain).

### cutToFirstSignificantSubdomainCustomWithWWW

Returns the part of the domain that includes top-level subdomains up to the first significant subdomain without stripping `www`.
Accepts custom TLD list name.
It can be useful if you need a fresh TLD list or if you have a custom list.

**Configuration example**

```xml
<!-- <top_level_domains_path>/var/lib/clickhouse/top_level_domains/</top_level_domains_path> -->
<top_level_domains_lists>
    <!-- https://publicsuffix.org/list/public_suffix_list.dat -->
    <public_suffix_list>public_suffix_list.dat</public_suffix_list>
    <!-- NOTE: path is under top_level_domains_path -->
</top_level_domains_lists>
```

**Syntax**

```sql
cutToFirstSignificantSubdomainCustomWithWWW(url, tld)
```

**Arguments**

- `url` — URL. [String](../../sql-reference/data-types/string.md).
- `tld` — Custom TLD list name. [String](../../sql-reference/data-types/string.md).

**Returned value**

- Part of the domain that includes top-level subdomains up to the first significant subdomain without stripping `www`. [String](../data-types/string.md).

**Example**

Query:

```sql
SELECT cutToFirstSignificantSubdomainCustomWithWWW('www.foo', 'public_suffix_list');
```

Result:

```text
┌─cutToFirstSignificantSubdomainCustomWithWWW('www.foo', 'public_suffix_list')─┐
│ www.foo                                                                      │
└──────────────────────────────────────────────────────────────────────────────┘
```

**See Also**

- [firstSignificantSubdomain](#firstsignificantsubdomain).

### cutToFirstSignificantSubdomainCustomWithWWWRFC

Returns the part of the domain that includes top-level subdomains up to the first significant subdomain without stripping `www`.
Accepts custom TLD list name.
It can be useful if you need a fresh TLD list or if you have a custom list.
Similar to [cutToFirstSignificantSubdomainCustomWithWWW](#cuttofirstsignificantsubdomaincustomwithwww) but conforms to RFC 3986.

**Syntax**

```sql
cutToFirstSignificantSubdomainCustomWithWWWRFC(url, tld)
```

**Arguments**

- `url` — URL. [String](../../sql-reference/data-types/string.md).
- `tld` — Custom TLD list name. [String](../../sql-reference/data-types/string.md).

**Returned value**

- Part of the domain that includes top-level subdomains up to the first significant subdomain without stripping `www`. [String](../../sql-reference/data-types/string.md).

**See Also**

- [firstSignificantSubdomain](#firstsignificantsubdomain).

### firstSignificantSubdomainCustom

Returns the first significant subdomain.
Accepts customs TLD list name.
Can be useful if you need fresh TLD list or you have custom.

Configuration example:

```xml
<!-- <top_level_domains_path>/var/lib/clickhouse/top_level_domains/</top_level_domains_path> -->
<top_level_domains_lists>
    <!-- https://publicsuffix.org/list/public_suffix_list.dat -->
    <public_suffix_list>public_suffix_list.dat</public_suffix_list>
    <!-- NOTE: path is under top_level_domains_path -->
</top_level_domains_lists>
```

**Syntax**

```sql
firstSignificantSubdomainCustom(url, tld)
```

**Arguments**

- `url` — URL. [String](../../sql-reference/data-types/string.md).
- `tld` — Custom TLD list name. [String](../../sql-reference/data-types/string.md).

**Returned value**

- First significant subdomain. [String](../../sql-reference/data-types/string.md).

**Example**

Query:

```sql
SELECT firstSignificantSubdomainCustom('bar.foo.there-is-no-such-domain', 'public_suffix_list');
```

Result:

```text
┌─firstSignificantSubdomainCustom('bar.foo.there-is-no-such-domain', 'public_suffix_list')─┐
│ foo                                                                                      │
└──────────────────────────────────────────────────────────────────────────────────────────┘
```

**See Also**

- [firstSignificantSubdomain](#firstsignificantsubdomain).

### firstSignificantSubdomainCustomRFC

Returns the first significant subdomain.
Accepts customs TLD list name.
Can be useful if you need fresh TLD list or you have custom.
Similar to [firstSignificantSubdomainCustom](#firstsignificantsubdomaincustom) but conforms to RFC 3986.

**Syntax**

```sql
firstSignificantSubdomainCustomRFC(url, tld)
```

**Arguments**

- `url` — URL. [String](../../sql-reference/data-types/string.md).
- `tld` — Custom TLD list name. [String](../../sql-reference/data-types/string.md).

**Returned value**

- First significant subdomain. [String](../../sql-reference/data-types/string.md).

**See Also**

- [firstSignificantSubdomain](#firstsignificantsubdomain).

### port

Returns the port or `default_port` if the URL contains no port or cannot be parsed.

**Syntax**

```sql
port(url [, default_port = 0])
```

**Arguments**

- `url` — URL. [String](../data-types/string.md).
- `default_port` — The default port number to be returned. [UInt16](../data-types/int-uint.md).

**Returned value**

- Port or the default port if there is no port in the URL or in case of a validation error. [UInt16](../data-types/int-uint.md).

**Example**

Query:

```sql
SELECT port('http://paul@www.example.com:80/');
```

Result:

```response
┌─port('http://paul@www.example.com:80/')─┐
│                                      80 │
└─────────────────────────────────────────┘
```

### portRFC

Returns the port or `default_port` if the URL contains no port or cannot be parsed.
Similar to [port](#port), but RFC 3986 conformant.

**Syntax**

```sql
portRFC(url [, default_port = 0])
```

**Arguments**

- `url` — URL. [String](../../sql-reference/data-types/string.md).
- `default_port` — The default port number to be returned. [UInt16](../data-types/int-uint.md).

**Returned value**

- Port or the default port if there is no port in the URL or in case of a validation error. [UInt16](../data-types/int-uint.md).

**Example**

Query:

```sql
SELECT
    port('http://user:password@example.com:8080'),
    portRFC('http://user:password@example.com:8080');
```

Result:

```resposne
┌─port('http://user:password@example.com:8080')─┬─portRFC('http://user:password@example.com:8080')─┐
│                                             0 │                                             8080 │
└───────────────────────────────────────────────┴──────────────────────────────────────────────────┘
```

### path

Returns the path without query string.

Example: `/top/news.html`.

### pathFull

The same as above, but including query string and fragment.

Example: `/top/news.html?page=2#comments`.

### queryString

Returns the query string without the initial question mark, `#` and everything after `#`.

Example: `page=1&lr=213`.

### fragment

Returns the fragment identifier without the initial hash symbol.

### queryStringAndFragment

Returns the query string and fragment identifier.

Example: `page=1#29390`.

### extractURLParameter(url, name)

Returns the value of the `name` parameter in the URL, if present, otherwise an empty string is returned.
If there are multiple parameters with this name, the first occurrence is returned.
The function assumes that the parameter in the `url` parameter is encoded in the same way as in the `name` argument.

### extractURLParameters(url)

Returns an array of `name=value` strings corresponding to the URL parameters.
The values are not decoded.

### extractURLParameterNames(url)

Returns an array of name strings corresponding to the names of URL parameters.
The values are not decoded.

### URLHierarchy(url)

Returns an array containing the URL, truncated at the end by the symbols /,? in the path and query-string.
Consecutive separator characters are counted as one.
The cut is made in the position after all the consecutive separator characters.

### URLPathHierarchy(url)

The same as above, but without the protocol and host in the result. The / element (root) is not included.

``` text
URLPathHierarchy('https://example.com/browse/CONV-6788') =
[
    '/browse/',
    '/browse/CONV-6788'
]
```

### encodeURLComponent(url)

Returns the encoded URL.

Example:

``` sql
SELECT encodeURLComponent('http://127.0.0.1:8123/?query=SELECT 1;') AS EncodedURL;
```

``` text
┌─EncodedURL───────────────────────────────────────────────┐
│ http%3A%2F%2F127.0.0.1%3A8123%2F%3Fquery%3DSELECT%201%3B │
└──────────────────────────────────────────────────────────┘
```

### decodeURLComponent(url)

Returns the decoded URL.

Example:

``` sql
SELECT decodeURLComponent('http://127.0.0.1:8123/?query=SELECT%201%3B') AS DecodedURL;
```

``` text
┌─DecodedURL─────────────────────────────┐
│ http://127.0.0.1:8123/?query=SELECT 1; │
└────────────────────────────────────────┘
```

### encodeURLFormComponent(url)

Returns the encoded URL. Follows rfc-1866, space(` `) is encoded as plus(`+`).

Example:

``` sql
SELECT encodeURLFormComponent('http://127.0.0.1:8123/?query=SELECT 1 2+3') AS EncodedURL;
```

``` text
┌─EncodedURL────────────────────────────────────────────────┐
│ http%3A%2F%2F127.0.0.1%3A8123%2F%3Fquery%3DSELECT+1+2%2B3 │
└───────────────────────────────────────────────────────────┘
```

### decodeURLFormComponent(url)

Returns the decoded URL. Follows rfc-1866, plain plus(`+`) is decoded as space(` `).

Example:

``` sql
SELECT decodeURLFormComponent('http://127.0.0.1:8123/?query=SELECT%201+2%2B3') AS DecodedURL;
```

``` text
┌─DecodedURL────────────────────────────────┐
│ http://127.0.0.1:8123/?query=SELECT 1 2+3 │
└───────────────────────────────────────────┘
```

### netloc

Extracts network locality (`username:password@host:port`) from a URL.

**Syntax**

``` sql
netloc(url)
```

**Arguments**

- `url` — URL. [String](../../sql-reference/data-types/string.md).

**Returned value**

- `username:password@host:port`. [String](../data-types/string.md).

**Example**

Query:

``` sql
SELECT netloc('http://paul@www.example.com:80/');
```

Result:

``` text
┌─netloc('http://paul@www.example.com:80/')─┐
│ paul@www.example.com:80                   │
└───────────────────────────────────────────┘
```

## Functions that remove part of a URL

If the URL does not have anything similar, the URL remains unchanged.

### cutWWW

Removes leading `www.` (if present) from the URL’s domain.

### cutQueryString

Removes query string, including the question mark.

### cutFragment

Removes the fragment identifier, including the number sign.

### cutQueryStringAndFragment

Removes the query string and fragment identifier, including the question mark and number sign.

### cutURLParameter(url, name)

Removes the `name` parameter from a URL, if present.
This function does not encode or decode characters in parameter names, e.g. `Client ID` and `Client%20ID` are treated as different parameter names.

**Syntax**

``` sql
cutURLParameter(url, name)
```

**Arguments**

- `url` — URL. [String](../../sql-reference/data-types/string.md).
- `name` — name of URL parameter. [String](../../sql-reference/data-types/string.md) or [Array](../../sql-reference/data-types/array.md) of Strings.

**Returned value**

- url with `name` URL parameter removed. [String](../data-types/string.md).

**Example**

Query:

``` sql
SELECT
    cutURLParameter('http://bigmir.net/?a=b&c=d&e=f#g', 'a') as url_without_a,
    cutURLParameter('http://bigmir.net/?a=b&c=d&e=f#g', ['c', 'e']) as url_without_c_and_e;
```

Result:

``` text
┌─url_without_a────────────────┬─url_without_c_and_e──────┐
│ http://bigmir.net/?c=d&e=f#g │ http://bigmir.net/?a=b#g │
└──────────────────────────────┴──────────────────────────┘
```
