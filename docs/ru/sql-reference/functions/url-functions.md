---
toc_priority: 54
toc_title: "Функции для работы с URL"
---

# Функции для работы с URL {#funktsii-dlia-raboty-s-url}

Все функции работают не по RFC - то есть, максимально упрощены ради производительности.

## Функции, извлекающие часть URL-а {#funktsii-izvlekaiushchie-chast-url-a}

Если в URL-е нет ничего похожего, то возвращается пустая строка.

### protocol {#protocol}

Возвращает протокол. Примеры: http, ftp, mailto, magnet…

### domain {#domain}

Извлекает имя хоста из URL.

``` sql
domain(url)
```

**Аргументы**

-   `url` — URL. Тип — [String](../../sql-reference/functions/url-functions.md).

URL может быть указан со схемой или без неё. Примеры:

``` text
svn+ssh://some.svn-hosting.com:80/repo/trunk
some.svn-hosting.com:80/repo/trunk
https://yandex.com/time/
```

Для указанных примеров функция `domain` возвращает следующие результаты:

``` text
some.svn-hosting.com
some.svn-hosting.com
yandex.com
```

**Возвращаемые значения**

-   Имя хоста. Если ClickHouse может распарсить входную строку как URL.
-   Пустая строка. Если ClickHouse не может распарсить входную строку как URL.

Тип — `String`.

**Пример**

``` sql
SELECT domain('svn+ssh://some.svn-hosting.com:80/repo/trunk');
```

``` text
┌─domain('svn+ssh://some.svn-hosting.com:80/repo/trunk')─┐
│ some.svn-hosting.com                                   │
└────────────────────────────────────────────────────────┘
```

### domainWithoutWWW {#domainwithoutwww}

Возвращает домен, удалив префикс ‘www.’, если он присутствовал.

### topLevelDomain {#topleveldomain}

Извлекает домен верхнего уровня из URL.

``` sql
topLevelDomain(url)
```

**Аргументы**

-   `url` — URL. Тип — [String](../../sql-reference/functions/url-functions.md).

URL может быть указан со схемой или без неё. Примеры:

``` text
svn+ssh://some.svn-hosting.com:80/repo/trunk
some.svn-hosting.com:80/repo/trunk
https://yandex.com/time/
```

**Возвращаемые значения**

-   Имя домена. Если ClickHouse может распарсить входную строку как URL.
-   Пустая строка. Если ClickHouse не может распарсить входную строку как URL.

Тип — `String`.

**Пример**

``` sql
SELECT topLevelDomain('svn+ssh://www.some.svn-hosting.com:80/repo/trunk');
```

``` text
┌─topLevelDomain('svn+ssh://www.some.svn-hosting.com:80/repo/trunk')─┐
│ com                                                                │
└────────────────────────────────────────────────────────────────────┘
```

### firstSignificantSubdomain {#firstsignificantsubdomain}

Возвращает «первый существенный поддомен». Это понятие является нестандартным и специфично для Яндекс.Метрики. Первый существенный поддомен - это домен второго уровня, если он не равен одному из com, net, org, co, или домен третьего уровня, иначе. Например, firstSignificantSubdomain(‘https://news.yandex.ru/’) = ‘yandex’, firstSignificantSubdomain(‘https://news.yandex.com.tr/’) = ‘yandex’. Список «несущественных» доменов второго уровня и другие детали реализации могут изменяться в будущем.

### cutToFirstSignificantSubdomain {#cuttofirstsignificantsubdomain}

Возвращает часть домена, включающую поддомены верхнего уровня до «первого существенного поддомена» (см. выше).

Например, `cutToFirstSignificantSubdomain('https://news.yandex.com.tr/') = 'yandex.com.tr'`.

### cutToFirstSignificantSubdomainCustom {#cuttofirstsignificantsubdomaincustom}

Возвращает часть домена, включающую поддомены верхнего уровня до первого существенного поддомена. Принимает имя пользовательского [списка доменов верхнего уровня](https://ru.wikipedia.org/wiki/Список_доменов_верхнего_уровня).

Полезно, если требуется актуальный список доменов верхнего уровня или если есть пользовательский.

Пример конфигурации:

```xml
<!-- <top_level_domains_path>/var/lib/clickhouse/top_level_domains/</top_level_domains_path> -->
<top_level_domains_lists>
    <!-- https://publicsuffix.org/list/public_suffix_list.dat -->
    <public_suffix_list>public_suffix_list.dat</public_suffix_list>
    <!-- NOTE: path is under top_level_domains_path -->
</top_level_domains_lists>
```

**Синтаксис**

``` sql
cutToFirstSignificantSubdomain(URL, TLD)
```

**Аргументы**

-   `URL` — URL. [String](../../sql-reference/data-types/string.md).
-   `TLD` — имя пользовательского списка доменов верхнего уровня. [String](../../sql-reference/data-types/string.md).

**Возвращаемое значение**

-   Часть домена, включающая поддомены верхнего уровня до первого существенного поддомена.

Тип: [String](../../sql-reference/data-types/string.md).

**Пример**

Запрос:

```sql
SELECT cutToFirstSignificantSubdomainCustom('bar.foo.there-is-no-such-domain', 'public_suffix_list');
```

Результат:

```text
┌─cutToFirstSignificantSubdomainCustom('bar.foo.there-is-no-such-domain', 'public_suffix_list')─┐
│ foo.there-is-no-such-domain                                                                   │
└───────────────────────────────────────────────────────────────────────────────────────────────┘
```

**Смотрите также**

-   [firstSignificantSubdomain](#firstsignificantsubdomain).

### cutToFirstSignificantSubdomainCustomWithWWW {#cuttofirstsignificantsubdomaincustomwithwww}

Возвращает часть домена, включающую поддомены верхнего уровня до первого существенного поддомена, не опуская "www". Принимает имя пользовательского списка доменов верхнего уровня.

Полезно, если требуется актуальный список доменов верхнего уровня или если есть пользовательский.

Пример конфигурации:

```xml
<!-- <top_level_domains_path>/var/lib/clickhouse/top_level_domains/</top_level_domains_path> -->
<top_level_domains_lists>
    <!-- https://publicsuffix.org/list/public_suffix_list.dat -->
    <public_suffix_list>public_suffix_list.dat</public_suffix_list>
    <!-- NOTE: path is under top_level_domains_path -->
</top_level_domains_lists>
```

**Синтаксис**

```sql
cutToFirstSignificantSubdomainCustomWithWWW(URL, TLD)
```

**Аргументы**

-   `URL` — URL. [String](../../sql-reference/data-types/string.md).
-   `TLD` — имя пользовательского списка доменов верхнего уровня. [String](../../sql-reference/data-types/string.md).

**Возвращаемое значение**

-   Часть домена, включающая поддомены верхнего уровня до первого существенного поддомена, без удаления `www`.

Тип: [String](../../sql-reference/data-types/string.md).

**Пример**

Запрос:

```sql
SELECT cutToFirstSignificantSubdomainCustomWithWWW('www.foo', 'public_suffix_list');
```

Результат:

```text
┌─cutToFirstSignificantSubdomainCustomWithWWW('www.foo', 'public_suffix_list')─┐
│ www.foo                                                                      │
└──────────────────────────────────────────────────────────────────────────────┘
```

**Смотрите также**

-   [firstSignificantSubdomain](#firstsignificantsubdomain).

### firstSignificantSubdomainCustom {#firstsignificantsubdomaincustom}

Возвращает первый существенный поддомен. Принимает имя пользовательского списка доменов верхнего уровня.

Полезно, если требуется актуальный список доменов верхнего уровня или если есть пользовательский.

Пример конфигурации:

```xml
<!-- <top_level_domains_path>/var/lib/clickhouse/top_level_domains/</top_level_domains_path> -->
<top_level_domains_lists>
    <!-- https://publicsuffix.org/list/public_suffix_list.dat -->
    <public_suffix_list>public_suffix_list.dat</public_suffix_list>
    <!-- NOTE: path is under top_level_domains_path -->
</top_level_domains_lists>
```

**Синтаксис**

```sql
firstSignificantSubdomainCustom(URL, TLD)
```

**Аргументы**

-   `URL` — URL. [String](../../sql-reference/data-types/string.md).
-   `TLD` — имя пользовательского списка доменов верхнего уровня. [String](../../sql-reference/data-types/string.md).

**Возвращаемое значение**

-   Первый существенный поддомен.

Тип: [String](../../sql-reference/data-types/string.md).

**Пример**

Запрос:

```sql
SELECT firstSignificantSubdomainCustom('bar.foo.there-is-no-such-domain', 'public_suffix_list');
```

Результат:

```text
┌─firstSignificantSubdomainCustom('bar.foo.there-is-no-such-domain', 'public_suffix_list')─┐
│ foo                                                                                      │
└──────────────────────────────────────────────────────────────────────────────────────────┘
```

**Смотрите также**

-   [firstSignificantSubdomain](#firstsignificantsubdomain).

### port(URL[, default_port = 0]) {#port}

Возвращает порт или значение `default_port`, если в URL-адресе нет порта (или передан невалидный URL)

### path {#path}

Возвращает путь. Пример: `/top/news.html` Путь не включает в себя query string.

### pathFull {#pathfull}

То же самое, но включая query string и fragment. Пример: /top/news.html?page=2#comments

### queryString {#querystring}

Возвращает query-string. Пример: page=1&lr=213. query-string не включает в себя начальный знак вопроса, а также # и всё, что после #.

### fragment {#fragment}

Возвращает fragment identifier. fragment не включает в себя начальный символ решётки.

### queryStringAndFragment {#querystringandfragment}

Возвращает query string и fragment identifier. Пример: страница=1#29390.

### extractURLParameter(URL, name) {#extracturlparameterurl-name}

Возвращает значение параметра name в URL, если такой есть; или пустую строку, иначе; если параметров с таким именем много - вернуть первый попавшийся. Функция работает при допущении, что имя параметра закодировано в URL в точности таким же образом, что и в переданном аргументе.

### extractURLParameters(URL) {#extracturlparametersurl}

Возвращает массив строк вида name=value, соответствующих параметрам URL. Значения никак не декодируются.

### extractURLParameterNames(URL) {#extracturlparameternamesurl}

Возвращает массив строк вида name, соответствующих именам параметров URL. Значения никак не декодируются.

### URLHierarchy(URL) {#urlhierarchyurl}

Возвращает массив, содержащий URL, обрезанный с конца по символам /, ? в пути и query-string. Подряд идущие символы-разделители считаются за один. Резка производится в позиции после всех подряд идущих символов-разделителей. Пример:

### URLPathHierarchy(URL) {#urlpathhierarchyurl}

То же самое, но без протокола и хоста в результате. Элемент / (корень) не включается. Пример:
Функция используется для реализации древовидных отчётов по URL в Яндекс.Метрике.

``` text
URLPathHierarchy('https://example.com/browse/CONV-6788') =
[
    '/browse/',
    '/browse/CONV-6788'
]
```

### decodeURLComponent(URL) {#decodeurlcomponenturl}

Возвращает декодированный URL.
Пример:

``` sql
SELECT decodeURLComponent('http://127.0.0.1:8123/?query=SELECT%201%3B') AS DecodedURL;
```

``` text
┌─DecodedURL─────────────────────────────┐
│ http://127.0.0.1:8123/?query=SELECT 1; │
└────────────────────────────────────────┘
```

### netloc {#netloc}

Извлекает сетевую локальность (`username:password@host:port`) из URL.

**Синтаксис**

```sql
netloc(URL)
```

**Аргументы**

-   `url` — URL. Тип — [String](../../sql-reference/data-types/string.md).

**Возвращаемое значение**

-   `username:password@host:port`.

Тип: `String`.

**Пример**

Запрос:

``` sql
SELECT netloc('http://paul@www.example.com:80/');
```

Результат:

``` text
┌─netloc('http://paul@www.example.com:80/')─┐
│ paul@www.example.com:80                   │
└───────────────────────────────────────────┘
```

## Функции, удаляющие часть из URL-а {#funktsii-udaliaiushchie-chast-iz-url-a}

Если в URL-е нет ничего похожего, то URL остаётся без изменений.

### cutWWW {#cutwww}

Удаляет не более одного ‘www.’ с начала домена URL-а, если есть.

### cutQueryString {#cutquerystring}

Удаляет query string. Знак вопроса тоже удаляется.

### cutFragment {#cutfragment}

Удаляет fragment identifier. Символ решётки тоже удаляется.

### cutQueryStringAndFragment {#cutquerystringandfragment}

Удаляет query string и fragment identifier. Знак вопроса и символ решётки тоже удаляются.

### cutURLParameter(URL, name) {#cuturlparameterurl-name}

Удаляет параметр URL с именем name, если такой есть. Функция работает при допущении, что имя параметра закодировано в URL в точности таким же образом, что и в переданном аргументе.

