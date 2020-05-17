---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 56
toc_title: Travailler avec JSON
---

# Fonctions pour travailler avec JSON {#functions-for-working-with-json}

Dans Yandex.Metrica, JSON est transmis par les utilisateurs en tant que paramètres de session. Il y a quelques fonctions spéciales pour travailler avec ce JSON. (Bien que dans la plupart des cas, les JSONs soient en outre prétraités et les valeurs résultantes sont placées dans des colonnes séparées dans leur format traité.) Toutes ces fonctions sont basées sur des hypothèses fortes sur ce que le JSON peut être, mais elles essaient de faire le moins possible pour faire le travail.

Les hypothèses suivantes sont apportées:

1.  Le nom du champ (argument de fonction) doit être une constante.
2.  Le nom du champ est en quelque sorte codé canoniquement dans JSON. Exemple: `visitParamHas('{"abc":"def"}', 'abc') = 1`, mais `visitParamHas('{"\\u0061\\u0062\\u0063":"def"}', 'abc') = 0`
3.  Les champs sont recherchés à n'importe quel niveau d'imbrication, sans discrimination. S'il y a plusieurs champs correspondants, la première occurrence est utilisé.
4.  Le JSON n'a pas de caractères d'espace en dehors des littéraux de chaîne.

## visitParamHas(params, nom) {#visitparamhasparams-name}

Vérifie s'il existe un champ avec ‘name’ nom.

## visitParamExtractUInt(params, nom) {#visitparamextractuintparams-name}

Analyse UInt64 à partir de la valeur du champ nommé ‘name’. Si c'est un champ de type chaîne, il tente d'analyser un numéro à partir du début de la chaîne. Si le champ n'existe pas, ou s'il existe mais ne contient pas de nombre, il renvoie 0.

## visitParamExtractInt(params, name) {#visitparamextractintparams-name}

Le même que pour Int64.

## visitParamExtractFloat(params, nom) {#visitparamextractfloatparams-name}

Le même que pour Float64.

## visitParamExtractBool(params, nom) {#visitparamextractboolparams-name}

Analyse d'une valeur vrai/faux. Le résultat est UInt8.

## visitParamExtractRaw(params, nom) {#visitparamextractrawparams-name}

Retourne la valeur d'un champ, y compris les séparateurs.

Exemple:

``` sql
visitParamExtractRaw('{"abc":"\\n\\u0000"}', 'abc') = '"\\n\\u0000"'
visitParamExtractRaw('{"abc":{"def":[1,2,3]}}', 'abc') = '{"def":[1,2,3]}'
```

## visitParamExtractString(params, nom) {#visitparamextractstringparams-name}

Analyse la chaîne entre guillemets doubles. La valeur est sans échappement. Si l'échappement échoue, il renvoie une chaîne vide.

Exemple:

``` sql
visitParamExtractString('{"abc":"\\n\\u0000"}', 'abc') = '\n\0'
visitParamExtractString('{"abc":"\\u263a"}', 'abc') = '☺'
visitParamExtractString('{"abc":"\\u263"}', 'abc') = ''
visitParamExtractString('{"abc":"hello}', 'abc') = ''
```

Il n'y a actuellement aucun support pour les points de code dans le format `\uXXXX\uYYYY` qui ne proviennent pas du plan multilingue de base (ils sont convertis en CESU-8 au lieu de UTF-8).

Les fonctions suivantes sont basées sur [simdjson](https://github.com/lemire/simdjson) conçu pour des exigences D'analyse JSON plus complexes. L'hypothèse 2 mentionnée ci-dessus s'applique toujours.

## isValidJSON (json) {#isvalidjsonjson}

Vérifie que la chaîne est un json valide.

Exemple:

``` sql
SELECT isValidJSON('{"a": "hello", "b": [-100, 200.0, 300]}') = 1
SELECT isValidJSON('not a json') = 0
```

## JSONHas(json\[, indices\_or\_keys\]…) {#jsonhasjson-indices-or-keys}

Si la valeur existe dans le document JSON, `1` sera retourné.

Si la valeur n'existe pas, `0` sera retourné.

Exemple:

``` sql
SELECT JSONHas('{"a": "hello", "b": [-100, 200.0, 300]}', 'b') = 1
SELECT JSONHas('{"a": "hello", "b": [-100, 200.0, 300]}', 'b', 4) = 0
```

`indices_or_keys` est une liste de zéro ou plusieurs arguments chacun d'entre eux peut être une chaîne ou un entier.

-   String = membre d'objet d'accès par clé.
-   Entier positif = accédez au n-ème membre / clé depuis le début.
-   Entier négatif = accédez au n-ème membre / clé à partir de la fin.

Minimum de l'indice de l'élément est 1. Ainsi, l'élément 0 n'existe pas.

Vous pouvez utiliser des entiers pour accéder à la fois aux tableaux JSON et aux objets JSON.

Ainsi, par exemple:

``` sql
SELECT JSONExtractKey('{"a": "hello", "b": [-100, 200.0, 300]}', 1) = 'a'
SELECT JSONExtractKey('{"a": "hello", "b": [-100, 200.0, 300]}', 2) = 'b'
SELECT JSONExtractKey('{"a": "hello", "b": [-100, 200.0, 300]}', -1) = 'b'
SELECT JSONExtractKey('{"a": "hello", "b": [-100, 200.0, 300]}', -2) = 'a'
SELECT JSONExtractString('{"a": "hello", "b": [-100, 200.0, 300]}', 1) = 'hello'
```

## JSONLength(json\[, indices\_or\_keys\]…) {#jsonlengthjson-indices-or-keys}

Renvoie la longueur D'un tableau JSON ou d'un objet JSON.

Si la valeur n'existe pas ou a un mauvais type, `0` sera retourné.

Exemple:

``` sql
SELECT JSONLength('{"a": "hello", "b": [-100, 200.0, 300]}', 'b') = 3
SELECT JSONLength('{"a": "hello", "b": [-100, 200.0, 300]}') = 2
```

## JSONType(json\[, indices\_or\_keys\]…) {#jsontypejson-indices-or-keys}

De retour le type d'une valeur JSON.

Si la valeur n'existe pas, `Null` sera retourné.

Exemple:

``` sql
SELECT JSONType('{"a": "hello", "b": [-100, 200.0, 300]}') = 'Object'
SELECT JSONType('{"a": "hello", "b": [-100, 200.0, 300]}', 'a') = 'String'
SELECT JSONType('{"a": "hello", "b": [-100, 200.0, 300]}', 'b') = 'Array'
```

## JSONExtractUInt(json\[, indices\_or\_keys\]…) {#jsonextractuintjson-indices-or-keys}

## JSONExtractInt(json\[, indices\_or\_keys\]…) {#jsonextractintjson-indices-or-keys}

## JSONExtractFloat(json\[, indices\_or\_keys\]…) {#jsonextractfloatjson-indices-or-keys}

## JSONExtractBool(json\[, indices\_or\_keys\]…) {#jsonextractbooljson-indices-or-keys}

Analyse un JSON et extrait une valeur. Ces fonctions sont similaires à `visitParam` fonction.

Si la valeur n'existe pas ou a un mauvais type, `0` sera retourné.

Exemple:

``` sql
SELECT JSONExtractInt('{"a": "hello", "b": [-100, 200.0, 300]}', 'b', 1) = -100
SELECT JSONExtractFloat('{"a": "hello", "b": [-100, 200.0, 300]}', 'b', 2) = 200.0
SELECT JSONExtractUInt('{"a": "hello", "b": [-100, 200.0, 300]}', 'b', -1) = 300
```

## JSONExtractString(json\[, indices\_or\_keys\]…) {#jsonextractstringjson-indices-or-keys}

Analyse un JSON et extrait une chaîne. Cette fonction est similaire à `visitParamExtractString` fonction.

Si la valeur n'existe pas ou a un mauvais type, une chaîne vide est retournée.

La valeur est sans échappement. Si l'échappement échoue, il renvoie une chaîne vide.

Exemple:

``` sql
SELECT JSONExtractString('{"a": "hello", "b": [-100, 200.0, 300]}', 'a') = 'hello'
SELECT JSONExtractString('{"abc":"\\n\\u0000"}', 'abc') = '\n\0'
SELECT JSONExtractString('{"abc":"\\u263a"}', 'abc') = '☺'
SELECT JSONExtractString('{"abc":"\\u263"}', 'abc') = ''
SELECT JSONExtractString('{"abc":"hello}', 'abc') = ''
```

## JSONExtract(json\[, indices\_or\_keys…\], Return\_type) {#jsonextractjson-indices-or-keys-return-type}

Analyse un JSON et extrait une valeur du type de données clickhouse donné.

C'est une généralisation de la précédente `JSONExtract<type>` fonction.
Cela signifie
`JSONExtract(..., 'String')` retourne exactement le même que `JSONExtractString()`,
`JSONExtract(..., 'Float64')` retourne exactement le même que `JSONExtractFloat()`.

Exemple:

``` sql
SELECT JSONExtract('{"a": "hello", "b": [-100, 200.0, 300]}', 'Tuple(String, Array(Float64))') = ('hello',[-100,200,300])
SELECT JSONExtract('{"a": "hello", "b": [-100, 200.0, 300]}', 'Tuple(b Array(Float64), a String)') = ([-100,200,300],'hello')
SELECT JSONExtract('{"a": "hello", "b": [-100, 200.0, 300]}', 'b', 'Array(Nullable(Int8))') = [-100, NULL, NULL]
SELECT JSONExtract('{"a": "hello", "b": [-100, 200.0, 300]}', 'b', 4, 'Nullable(Int64)') = NULL
SELECT JSONExtract('{"passed": true}', 'passed', 'UInt8') = 1
SELECT JSONExtract('{"day": "Thursday"}', 'day', 'Enum8(\'Sunday\' = 0, \'Monday\' = 1, \'Tuesday\' = 2, \'Wednesday\' = 3, \'Thursday\' = 4, \'Friday\' = 5, \'Saturday\' = 6)') = 'Thursday'
SELECT JSONExtract('{"day": 5}', 'day', 'Enum8(\'Sunday\' = 0, \'Monday\' = 1, \'Tuesday\' = 2, \'Wednesday\' = 3, \'Thursday\' = 4, \'Friday\' = 5, \'Saturday\' = 6)') = 'Friday'
```

## JSONExtractKeysAndValues(json\[, indices\_or\_keys…\], Value\_type) {#jsonextractkeysandvaluesjson-indices-or-keys-value-type}

Analyse les paires clé-valeur à partir D'un JSON où les valeurs sont du type de données clickhouse donné.

Exemple:

``` sql
SELECT JSONExtractKeysAndValues('{"x": {"a": 5, "b": 7, "c": 11}}', 'x', 'Int8') = [('a',5),('b',7),('c',11)]
```

## JSONExtractRaw(json\[, indices\_or\_keys\]…) {#jsonextractrawjson-indices-or-keys}

Renvoie une partie de JSON en tant que chaîne non analysée.

Si la pièce n'existe pas ou a un mauvais type, une chaîne vide est retournée.

Exemple:

``` sql
SELECT JSONExtractRaw('{"a": "hello", "b": [-100, 200.0, 300]}', 'b') = '[-100, 200.0, 300]'
```

## JSONExtractArrayRaw(json\[, indices\_or\_keys…\]) {#jsonextractarrayrawjson-indices-or-keys}

Retourne un tableau avec des éléments de tableau JSON, chacun représenté comme une chaîne non analysée.

Si la pièce n'existe pas ou n'est pas de tableau, un tableau vide sera retournée.

Exemple:

``` sql
SELECT JSONExtractArrayRaw('{"a": "hello", "b": [-100, 200.0, "hello"]}', 'b') = ['-100', '200.0', '"hello"']'
```

## JSONExtractKeysAndValuesRaw {#json-extract-keys-and-values-raw}

Extrait les données brutes d'un objet JSON.

**Syntaxe**

``` sql
JSONExtractKeysAndValuesRaw(json[, p, a, t, h])
```

**Paramètre**

-   `json` — [Chaîne](../data-types/string.md) avec JSON valide.
-   `p, a, t, h` — Comma-separated indices or keys that specify the path to the inner field in a nested JSON object. Each argument can be either a [chaîne](../data-types/string.md) pour obtenir le champ par la touche ou un [entier](../data-types/int-uint.md) pour obtenir le N-ème champ (indexé à partir de 1, les entiers négatifs comptent à partir de la fin). S'il n'est pas défini, le JSON entier est analysé en tant qu'objet de niveau supérieur. Paramètre facultatif.

**Valeurs renvoyées**

-   Tableau avec `('key', 'value')` tuple. Les deux membres du tuple sont des chaînes.
-   Tableau vide si l'objet demandé n'existe pas, ou entrée JSON n'est pas valide.

Type: [Tableau](../data-types/array.md)([Tuple](../data-types/tuple.md)([Chaîne](../data-types/string.md), [Chaîne](../data-types/string.md)).

**Exemple**

Requête:

``` sql
SELECT JSONExtractKeysAndValuesRaw('{"a": [-100, 200.0], "b":{"c": {"d": "hello", "f": "world"}}}')
```

Résultat:

``` text
┌─JSONExtractKeysAndValuesRaw('{"a": [-100, 200.0], "b":{"c": {"d": "hello", "f": "world"}}}')─┐
│ [('a','[-100,200]'),('b','{"c":{"d":"hello","f":"world"}}')]                                 │
└──────────────────────────────────────────────────────────────────────────────────────────────┘
```

Requête:

``` sql
SELECT JSONExtractKeysAndValuesRaw('{"a": [-100, 200.0], "b":{"c": {"d": "hello", "f": "world"}}}', 'b')
```

Résultat:

``` text
┌─JSONExtractKeysAndValuesRaw('{"a": [-100, 200.0], "b":{"c": {"d": "hello", "f": "world"}}}', 'b')─┐
│ [('c','{"d":"hello","f":"world"}')]                                                               │
└───────────────────────────────────────────────────────────────────────────────────────────────────┘
```

Requête:

``` sql
SELECT JSONExtractKeysAndValuesRaw('{"a": [-100, 200.0], "b":{"c": {"d": "hello", "f": "world"}}}', -1, 'c')
```

Résultat:

``` text
┌─JSONExtractKeysAndValuesRaw('{"a": [-100, 200.0], "b":{"c": {"d": "hello", "f": "world"}}}', -1, 'c')─┐
│ [('d','"hello"'),('f','"world"')]                                                                     │
└───────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

[Article Original](https://clickhouse.tech/docs/en/query_language/functions/json_functions/) <!--hide-->
