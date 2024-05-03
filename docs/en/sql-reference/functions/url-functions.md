---
slug: /en/sql-reference/functions/url-functions
sidebar_position: 200
sidebar_label: URLs
---

# Functions for Working with URLs

All these functions do not follow the RFC. They are maximally simplified for improved performance.

## Functions that Extract Parts of a URL

If the relevant part isn’t present in a URL, an empty string is returned.

### protocol

Extracts the protocol from a URL.

Examples of typical returned values: http, https, ftp, mailto, tel, magnet…

### domain

Extracts the hostname from a URL.

``` sql
domain(url)
```

**Arguments**

- `url` — URL. Type: [String](../../sql-reference/data-types/string.md).

The URL can be specified with or without a scheme. Examples:

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

- Host name. If ClickHouse can parse the input string as a URL.
- Empty string. If ClickHouse can’t parse the input string as a URL.

Type: `String`.

**Example**

``` sql
SELECT domain('svn+ssh://some.svn-hosting.com:80/repo/trunk');
```

``` text
┌─domain('svn+ssh://some.svn-hosting.com:80/repo/trunk')─┐
│ some.svn-hosting.com                                   │
└────────────────────────────────────────────────────────┘
```

### domainWithoutWWW

Returns the domain and removes no more than one ‘www.’ from the beginning of it, if present.

### topLevelDomain

Extracts the the top-level domain from a URL.

``` sql
topLevelDomain(url)
```

**Arguments**

- `url` — URL. Type: [String](../../sql-reference/data-types/string.md).

The URL can be specified with or without a scheme. Examples:

``` text
svn+ssh://some.svn-hosting.com:80/repo/trunk
some.svn-hosting.com:80/repo/trunk
https://clickhouse.com/time/
```

**Returned values**

- Domain name. If ClickHouse can parse the input string as a URL.
- Empty string. If ClickHouse cannot parse the input string as a URL.

Type: `String`.

**Example**

``` sql
SELECT topLevelDomain('svn+ssh://www.some.svn-hosting.com:80/repo/trunk');
```

``` text
┌─topLevelDomain('svn+ssh://www.some.svn-hosting.com:80/repo/trunk')─┐
│ com                                                                │
└────────────────────────────────────────────────────────────────────┘
```

### firstSignificantSubdomain

Returns the “first significant subdomain”. The first significant subdomain is a second-level domain if it is ‘com’, ‘net’, ‘org’, or ‘co’. Otherwise, it is a third-level domain. For example, `firstSignificantSubdomain (‘https://news.clickhouse.com/’) = ‘clickhouse’, firstSignificantSubdomain (‘https://news.clickhouse.com.tr/’) = ‘clickhouse’`. The list of “insignificant” second-level domains and other implementation details may change in the future.

### cutToFirstSignificantSubdomain

Returns the part of the domain that includes top-level subdomains up to the “first significant subdomain” (see the explanation above).

For example:

- `cutToFirstSignificantSubdomain('https://news.clickhouse.com.tr/') = 'clickhouse.com.tr'`.
- `cutToFirstSignificantSubdomain('www.tr') = 'tr'`.
- `cutToFirstSignificantSubdomain('tr') = ''`.

### cutToFirstSignificantSubdomainWithWWW

Returns the part of the domain that includes top-level subdomains up to the “first significant subdomain”, without stripping "www".

For example:

- `cutToFirstSignificantSubdomain('https://news.clickhouse.com.tr/') = 'clickhouse.com.tr'`.
- `cutToFirstSignificantSubdomain('www.tr') = 'www.tr'`.
- `cutToFirstSignificantSubdomain('tr') = ''`.

### cutToFirstSignificantSubdomainCustom

Returns the part of the domain that includes top-level subdomains up to the first significant subdomain. Accepts custom [TLD list](https://en.wikipedia.org/wiki/List_of_Internet_top-level_domains) name.

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

``` sql
cutToFirstSignificantSubdomain(URL, TLD)
```

**Parameters**

- `URL` — URL. [String](../../sql-reference/data-types/string.md).
- `TLD` — Custom TLD list name. [String](../../sql-reference/data-types/string.md).

**Returned value**

- Part of the domain that includes top-level subdomains up to the first significant subdomain.

Type: [String](../../sql-reference/data-types/string.md).

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

### cutToFirstSignificantSubdomainCustomWithWWW

Returns the part of the domain that includes top-level subdomains up to the first significant subdomain without stripping `www`. Accepts custom TLD list name.

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
cutToFirstSignificantSubdomainCustomWithWWW(URL, TLD)
```

**Parameters**

- `URL` — URL. [String](../../sql-reference/data-types/string.md).
- `TLD` — Custom TLD list name. [String](../../sql-reference/data-types/string.md).

**Returned value**

- Part of the domain that includes top-level subdomains up to the first significant subdomain without stripping `www`.

Type: [String](../../sql-reference/data-types/string.md).

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

### firstSignificantSubdomainCustom

Returns the first significant subdomain. Accepts customs TLD list name.

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
firstSignificantSubdomainCustom(URL, TLD)
```

**Parameters**

- `URL` — URL. [String](../../sql-reference/data-types/string.md).
- `TLD` — Custom TLD list name. [String](../../sql-reference/data-types/string.md).

**Returned value**

- First significant subdomain.

Type: [String](../../sql-reference/data-types/string.md).

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

### port(URL\[, default_port = 0\])

Returns the port or `default_port` if there is no port in the URL (or in case of validation error).

### path

Returns the path. Example: `/top/news.html` The path does not include the query string.

### pathFull

The same as above, but including query string and fragment. Example: /top/news.html?page=2#comments

### queryString

Returns the query string. Example: page=1&lr=213. query-string does not include the initial question mark, as well as # and everything after #.

### fragment

Returns the fragment identifier. fragment does not include the initial hash symbol.

### queryStringAndFragment

Returns the query string and fragment identifier. Example: page=1#29390.

### extractURLParameter(URL, name)

Returns the value of the ‘name’ parameter in the URL, if present. Otherwise, an empty string. If there are many parameters with this name, it returns the first occurrence. This function works under the assumption that the parameter name is encoded in the URL exactly the same way as in the passed argument.

### extractURLParameters(URL)

Returns an array of name=value strings corresponding to the URL parameters. The values are not decoded in any way.

### extractURLParameterNames(URL)

Returns an array of name strings corresponding to the names of URL parameters. The values are not decoded in any way.

### URLHierarchy(URL)

Returns an array containing the URL, truncated at the end by the symbols /,? in the path and query-string. Consecutive separator characters are counted as one. The cut is made in the position after all the consecutive separator characters.

### URLPathHierarchy(URL)

The same as above, but without the protocol and host in the result. The / element (root) is not included.

``` text
URLPathHierarchy('https://example.com/browse/CONV-6788') =
[
    '/browse/',
    '/browse/CONV-6788'
]
```

### encodeURLComponent(URL)

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

### decodeURLComponent(URL)

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

### encodeURLFormComponent(URL)

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

### decodeURLFormComponent(URL)

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
netloc(URL)
```

**Arguments**

- `url` — URL. [String](../../sql-reference/data-types/string.md).

**Returned value**

- `username:password@host:port`.

Type: `String`.

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

## Functions that Remove Part of a URL

If the URL does not have anything similar, the URL remains unchanged.

### cutWWW

Removes no more than one ‘www.’ from the beginning of the URL’s domain, if present.

### cutQueryString

Removes query string. The question mark is also removed.

### cutFragment

Removes the fragment identifier. The number sign is also removed.

### cutQueryStringAndFragment

Removes the query string and fragment identifier. The question mark and number sign are also removed.

### cutURLParameter(URL, name)

Removes the `name` parameter from URL, if present. This function does not encode or decode characters in parameter names, e.g. `Client ID` and `Client%20ID` are treated as different parameter names.

**Syntax**

``` sql
cutURLParameter(URL, name)
```

**Arguments**

- `url` — URL. [String](../../sql-reference/data-types/string.md).
- `name` — name of URL parameter. [String](../../sql-reference/data-types/string.md) or [Array](../../sql-reference/data-types/array.md) of Strings.

**Returned value**

- URL with `name` URL parameter removed.

Type: `String`.

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
