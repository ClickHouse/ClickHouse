---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 54
toc_title: Travailler avec des URL
---

# Fonctions pour travailler avec des URL {#functions-for-working-with-urls}

Toutes ces fonctions ne suivent pas la RFC. Ils sont simplifiés au maximum pour améliorer les performances.

## Fonctions qui extraient des parties d'une URL {#functions-that-extract-parts-of-a-url}

Si la partie pertinente n'est pas présente dans une URL, une chaîne vide est renvoyée.

### protocole {#protocol}

Extrait le protocole d'une URL.

Examples of typical returned values: http, https, ftp, mailto, tel, magnet…

### domaine {#domain}

Extrait le nom d'hôte d'une URL.

``` sql
domain(url)
```

**Paramètre**

-   `url` — URL. Type: [Chaîne](../../sql-reference/data-types/string.md).

L'URL peut être spécifiée avec ou sans schéma. Exemple:

``` text
svn+ssh://some.svn-hosting.com:80/repo/trunk
some.svn-hosting.com:80/repo/trunk
https://yandex.com/time/
```

Pour ces exemples, le `domain` la fonction renvoie les résultats suivants:

``` text
some.svn-hosting.com
some.svn-hosting.com
yandex.com
```

**Valeurs renvoyées**

-   Nom d'hôte. Si ClickHouse peut analyser la chaîne d'entrée en tant QU'URL.
-   Chaîne vide. Si ClickHouse ne peut pas analyser la chaîne d'entrée en tant QU'URL.

Type: `String`.

**Exemple**

``` sql
SELECT domain('svn+ssh://some.svn-hosting.com:80/repo/trunk')
```

``` text
┌─domain('svn+ssh://some.svn-hosting.com:80/repo/trunk')─┐
│ some.svn-hosting.com                                   │
└────────────────────────────────────────────────────────┘
```

### domainWithoutWWW {#domainwithoutwww}

Renvoie le domaine et ne supprime pas plus d'un ‘www.’ dès le début de celui-ci, si présent.

### topLevelDomain {#topleveldomain}

Extrait le domaine de premier niveau d'une URL.

``` sql
topLevelDomain(url)
```

**Paramètre**

-   `url` — URL. Type: [Chaîne](../../sql-reference/data-types/string.md).

L'URL peut être spécifiée avec ou sans schéma. Exemple:

``` text
svn+ssh://some.svn-hosting.com:80/repo/trunk
some.svn-hosting.com:80/repo/trunk
https://yandex.com/time/
```

**Valeurs renvoyées**

-   Nom de domaine. Si ClickHouse peut analyser la chaîne d'entrée en tant QU'URL.
-   Chaîne vide. Si ClickHouse ne peut pas analyser la chaîne d'entrée en tant QU'URL.

Type: `String`.

**Exemple**

``` sql
SELECT topLevelDomain('svn+ssh://www.some.svn-hosting.com:80/repo/trunk')
```

``` text
┌─topLevelDomain('svn+ssh://www.some.svn-hosting.com:80/repo/trunk')─┐
│ com                                                                │
└────────────────────────────────────────────────────────────────────┘
```

### firstSignificantSubdomain {#firstsignificantsubdomain}

Renvoie la “first significant subdomain”. C'est un concept non standard spécifique à Yandex.Metrica. Le premier sous-domaine significatif est un domaine de deuxième niveau s'il est ‘com’, ‘net’, ‘org’, ou ‘co’. Sinon, il est un domaine de troisième niveau. Exemple, `firstSignificantSubdomain (‘https://news.yandex.ru/’) = ‘yandex’, firstSignificantSubdomain (‘https://news.yandex.com.tr/’) = ‘yandex’`. La liste des “insignificant” les domaines de deuxième niveau et d'autres détails de mise en œuvre peuvent changer à l'avenir.

### cutToFirstSignificantSubdomain {#cuttofirstsignificantsubdomain}

Renvoie la partie du domaine qui inclut les sous-domaines de premier niveau “first significant subdomain” (voir l'explication ci-dessus).

Exemple, `cutToFirstSignificantSubdomain('https://news.yandex.com.tr/') = 'yandex.com.tr'`.

### chemin {#path}

Retourne le chemin d'accès. Exemple: `/top/news.html` Le chemin n'inclut pas la chaîne de requête.

### pathFull {#pathfull}

La même chose que ci-dessus, mais y compris la chaîne de requête et le fragment. Exemple: / top / nouvelles.le html?page = 2 \# commentaires

### queryString {#querystring}

Retourne la chaîne de requête. Exemple: page = 1 & lr=213. query-string n'inclut pas le point d'interrogation initial, ainsi que \# et tout ce qui suit \#.

### fragment {#fragment}

Renvoie l'identificateur de fragment. fragment n'inclut pas le symbole de hachage initial.

### queryStringAndFragment {#querystringandfragment}

Renvoie la chaîne de requête et l'Identificateur de fragment. Exemple: page = 1 \# 29390.

### extractURLParameter (URL, nom) {#extracturlparameterurl-name}

Renvoie la valeur de la ‘name’ paramètre dans l'URL, le cas échéant. Sinon, une chaîne vide. S'il y a beaucoup de paramètres avec ce nom, il renvoie la première occurrence. Cette fonction fonctionne en supposant que le nom du paramètre est codé dans L'URL exactement de la même manière que dans l'argument passé.

### extractURLParameters (URL) {#extracturlparametersurl}

Renvoie un tableau de chaînes name = value correspondant aux paramètres D'URL. Les valeurs ne sont en aucun cas décodées.

### extractURLParameterNames (URL) {#extracturlparameternamesurl}

Retourne un tableau de chaînes de noms correspondant aux noms des paramètres d'URL. Les valeurs ne sont en aucun cas décodées.

### URLHierarchy (URL) {#urlhierarchyurl}

Retourne un tableau contenant L'URL, tronquée à la fin par les symboles /,? dans le chemin et la chaîne de requête. Les caractères séparateurs consécutifs sont comptés comme un. La coupe est faite dans la position après tous les caractères de séparation consécutifs.

### URLPathHierarchy (URL) {#urlpathhierarchyurl}

La même chose que ci-dessus, mais sans le protocole et l'hôte dans le résultat. Le / les élément (racine) n'est pas inclus. Exemple: la fonction est utilisée pour implémenter l'arborescence des rapports de L'URL dans Yandex. Métrique.

``` text
URLPathHierarchy('https://example.com/browse/CONV-6788') =
[
    '/browse/',
    '/browse/CONV-6788'
]
```

### decodeURLComponent (URL) {#decodeurlcomponenturl}

Renvoie L'URL décodée.
Exemple:

``` sql
SELECT decodeURLComponent('http://127.0.0.1:8123/?query=SELECT%201%3B') AS DecodedURL;
```

``` text
┌─DecodedURL─────────────────────────────┐
│ http://127.0.0.1:8123/?query=SELECT 1; │
└────────────────────────────────────────┘
```

## Fonctions qui suppriment une partie D'une URL {#functions-that-remove-part-of-a-url}

Si L'URL n'a rien de similaire, L'URL reste inchangée.

### cutWWW {#cutwww}

Supprime pas plus d'une ‘www.’ depuis le début du domaine de L'URL, s'il est présent.

### cutQueryString {#cutquerystring}

Supprime la chaîne de requête. Le point d'interrogation est également supprimé.

### cutFragment {#cutfragment}

Supprime l'identificateur de fragment. Le signe est également supprimé.

### couperystringandfragment {#cutquerystringandfragment}

Supprime la chaîne de requête et l'Identificateur de fragment. Le point d'interrogation et le signe numérique sont également supprimés.

### cutURLParameter (URL, nom) {#cuturlparameterurl-name}

Supprime le ‘name’ Paramètre URL, si présent. Cette fonction fonctionne en supposant que le nom du paramètre est codé dans L'URL exactement de la même manière que dans l'argument passé.

[Article Original](https://clickhouse.tech/docs/en/query_language/functions/url_functions/) <!--hide-->
