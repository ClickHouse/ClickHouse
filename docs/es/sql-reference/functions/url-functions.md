---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 54
toc_title: Trabajar con URL
---

# Funciones para trabajar con URL {#functions-for-working-with-urls}

Todas estas funciones no siguen el RFC. Se simplifican al máximo para mejorar el rendimiento.

## Funciones que extraen partes de una URL {#functions-that-extract-parts-of-a-url}

Si la parte relevante no está presente en una URL, se devuelve una cadena vacía.

### protocolo {#protocol}

Extrae el protocolo de una URL.

Examples of typical returned values: http, https, ftp, mailto, tel, magnet…

### dominio {#domain}

Extrae el nombre de host de una dirección URL.

``` sql
domain(url)
```

**Parámetros**

-   `url` — URL. Type: [Cadena](../../sql-reference/data-types/string.md).

La URL se puede especificar con o sin un esquema. Ejemplos:

``` text
svn+ssh://some.svn-hosting.com:80/repo/trunk
some.svn-hosting.com:80/repo/trunk
https://yandex.com/time/
```

Para estos ejemplos, el `domain` función devuelve los siguientes resultados:

``` text
some.svn-hosting.com
some.svn-hosting.com
yandex.com
```

**Valores devueltos**

-   Nombre de host. Si ClickHouse puede analizar la cadena de entrada como una URL.
-   Cadena vacía. Si ClickHouse no puede analizar la cadena de entrada como una URL.

Tipo: `String`.

**Ejemplo**

``` sql
SELECT domain('svn+ssh://some.svn-hosting.com:80/repo/trunk')
```

``` text
┌─domain('svn+ssh://some.svn-hosting.com:80/repo/trunk')─┐
│ some.svn-hosting.com                                   │
└────────────────────────────────────────────────────────┘
```

### Nuestros servicios {#domainwithoutwww}

Devuelve el dominio y no elimina más de uno ‘www.’ desde el principio de la misma, si está presente.

### topLevelDomain {#topleveldomain}

Extrae el dominio de nivel superior de una URL.

``` sql
topLevelDomain(url)
```

**Parámetros**

-   `url` — URL. Type: [Cadena](../../sql-reference/data-types/string.md).

La URL se puede especificar con o sin un esquema. Ejemplos:

``` text
svn+ssh://some.svn-hosting.com:80/repo/trunk
some.svn-hosting.com:80/repo/trunk
https://yandex.com/time/
```

**Valores devueltos**

-   Nombre de dominio. Si ClickHouse puede analizar la cadena de entrada como una URL.
-   Cadena vacía. Si ClickHouse no puede analizar la cadena de entrada como una URL.

Tipo: `String`.

**Ejemplo**

``` sql
SELECT topLevelDomain('svn+ssh://www.some.svn-hosting.com:80/repo/trunk')
```

``` text
┌─topLevelDomain('svn+ssh://www.some.svn-hosting.com:80/repo/trunk')─┐
│ com                                                                │
└────────────────────────────────────────────────────────────────────┘
```

### FirstSignificantSubdomain {#firstsignificantsubdomain}

Devuelve el “first significant subdomain”. Este es un concepto no estándar específico de Yandex.Métrica. El primer subdominio significativo es un dominio de segundo nivel si es ‘com’, ‘net’, ‘org’, o ‘co’. De lo contrario, es un dominio de tercer nivel. Por ejemplo, `firstSignificantSubdomain (‘https://news.yandex.ru/’) = ‘yandex’, firstSignificantSubdomain (‘https://news.yandex.com.tr/’) = ‘yandex’`. La lista de “insignificant” dominios de segundo nivel y otros detalles de implementación pueden cambiar en el futuro.

### cutToFirstSignificantSubdomain {#cuttofirstsignificantsubdomain}

Devuelve la parte del dominio que incluye subdominios de nivel superior “first significant subdomain” (véase la explicación anterior).

Por ejemplo, `cutToFirstSignificantSubdomain('https://news.yandex.com.tr/') = 'yandex.com.tr'`.

### camino {#path}

Devuelve la ruta de acceso. Ejemplo: `/top/news.html` La ruta de acceso no incluye la cadena de consulta.

### pathFull {#pathfull}

Lo mismo que el anterior, pero incluyendo cadena de consulta y fragmento. Ejemplo: /top/news.html?Página = 2 # comentarios

### queryString {#querystring}

Devuelve la cadena de consulta. Ejemplo: page=1&lr=213. query-string no incluye el signo de interrogación inicial, así como # y todo después de #.

### fragmento {#fragment}

Devuelve el identificador de fragmento. el fragmento no incluye el símbolo hash inicial.

### queryStringAndFragment {#querystringandfragment}

Devuelve la cadena de consulta y el identificador de fragmento. Ejemplo: page=1#29390.

### extractURLParameter(URL, nombre) {#extracturlparameterurl-name}

Devuelve el valor de la ‘name’ parámetro en la URL, si está presente. De lo contrario, una cadena vacía. Si hay muchos parámetros con este nombre, devuelve la primera aparición. Esta función funciona bajo el supuesto de que el nombre del parámetro está codificado en la URL exactamente de la misma manera que en el argumento pasado.

### extractURLParameters (URL) {#extracturlparametersurl}

Devuelve una matriz de cadenas name=value correspondientes a los parámetros de URL. Los valores no se decodifican de ninguna manera.

### ExtractURLParameterNames (URL) {#extracturlparameternamesurl}

Devuelve una matriz de cadenas de nombre correspondientes a los nombres de los parámetros de URL. Los valores no se decodifican de ninguna manera.

### URLJerarquía (URL) {#urlhierarchyurl}

Devuelve una matriz que contiene la URL, truncada al final por los símbolos /,? en la ruta y la cadena de consulta. Los caracteres separadores consecutivos se cuentan como uno. El corte se realiza en la posición después de todos los caracteres separadores consecutivos.

### URLPathHierarchy (URL) {#urlpathhierarchyurl}

Lo mismo que el anterior, pero sin el protocolo y el host en el resultado. El elemento / (raíz) no está incluido. Ejemplo: la función se utiliza para implementar informes de árbol de la URL en Yandex. Métrica.

``` text
URLPathHierarchy('https://example.com/browse/CONV-6788') =
[
    '/browse/',
    '/browse/CONV-6788'
]
```

### decodeURLComponent (URL) {#decodeurlcomponenturl}

Devuelve la dirección URL decodificada.
Ejemplo:

``` sql
SELECT decodeURLComponent('http://127.0.0.1:8123/?query=SELECT%201%3B') AS DecodedURL;
```

``` text
┌─DecodedURL─────────────────────────────┐
│ http://127.0.0.1:8123/?query=SELECT 1; │
└────────────────────────────────────────┘
```

## Funciones que eliminan parte de una URL {#functions-that-remove-part-of-a-url}

Si la URL no tiene nada similar, la URL permanece sin cambios.

### Sistema abierto {#cutwww}

Elimina no más de uno ‘www.’ desde el principio del dominio de la URL, si está presente.

### cutQueryString {#cutquerystring}

Quita la cadena de consulta. El signo de interrogación también se elimina.

### cutFragment {#cutfragment}

Quita el identificador de fragmento. El signo de número también se elimina.

### cutQueryStringAndFragment {#cutquerystringandfragment}

Quita la cadena de consulta y el identificador de fragmento. El signo de interrogación y el signo de número también se eliminan.

### cutURLParameter(URL, nombre) {#cuturlparameterurl-name}

Elimina el ‘name’ Parámetro URL, si está presente. Esta función funciona bajo el supuesto de que el nombre del parámetro está codificado en la URL exactamente de la misma manera que en el argumento pasado.

[Artículo Original](https://clickhouse.tech/docs/en/query_language/functions/url_functions/) <!--hide-->
