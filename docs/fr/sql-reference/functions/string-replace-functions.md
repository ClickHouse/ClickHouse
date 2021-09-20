---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 42
toc_title: "Pour remplacer dans les cha\xEEnes"
---

# Fonctions de recherche et de remplacement dans les chaînes {#functions-for-searching-and-replacing-in-strings}

## replaceOne(botte de foin, modèle, remplacement) {#replaceonehaystack-pattern-replacement}

Remplace la première occurrence, si elle existe, ‘pattern’ sous-chaîne dans ‘haystack’ avec l' ‘replacement’ substring.
Ci-après, ‘pattern’ et ‘replacement’ doivent être constantes.

## replaceAll(botte de foin, motif, remplacement), Remplacer(botte de foin, motif, remplacement) {#replaceallhaystack-pattern-replacement-replacehaystack-pattern-replacement}

Remplace toutes les occurrences du ‘pattern’ sous-chaîne dans ‘haystack’ avec l' ‘replacement’ substring.

## replaceRegexpOne(botte de foin, modèle, remplacement) {#replaceregexponehaystack-pattern-replacement}

Remplacement en utilisant le ‘pattern’ expression régulière. Une expression régulière re2.
Remplace seulement la première occurrence, si elle existe.
Un motif peut être spécifié comme ‘replacement’. Ce modèle peut inclure des substitutions `\0-\9`.
Substitution `\0` inclut l'expression régulière entière. Substitution `\1-\9` correspond au sous-modèle numbers.To utilisez le `\` caractère dans un modèle, échappez-le en utilisant `\`.
Aussi garder à l'esprit qu'un littéral de chaîne nécessite une évasion.

Exemple 1. Conversion de la date au format américain:

``` sql
SELECT DISTINCT
    EventDate,
    replaceRegexpOne(toString(EventDate), '(\\d{4})-(\\d{2})-(\\d{2})', '\\2/\\3/\\1') AS res
FROM test.hits
LIMIT 7
FORMAT TabSeparated
```

``` text
2014-03-17      03/17/2014
2014-03-18      03/18/2014
2014-03-19      03/19/2014
2014-03-20      03/20/2014
2014-03-21      03/21/2014
2014-03-22      03/22/2014
2014-03-23      03/23/2014
```

Exemple 2. Copier une chaîne dix fois:

``` sql
SELECT replaceRegexpOne('Hello, World!', '.*', '\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0') AS res
```

``` text
┌─res────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ Hello, World!Hello, World!Hello, World!Hello, World!Hello, World!Hello, World!Hello, World!Hello, World!Hello, World!Hello, World! │
└────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

## replaceRegexpAll(botte de foin, modèle, remplacement) {#replaceregexpallhaystack-pattern-replacement}

Cela fait la même chose, mais remplace toutes les occurrences. Exemple:

``` sql
SELECT replaceRegexpAll('Hello, World!', '.', '\\0\\0') AS res
```

``` text
┌─res────────────────────────┐
│ HHeelllloo,,  WWoorrlldd!! │
└────────────────────────────┘
```

Par exception, si une expression régulière travaillé sur un vide sous-chaîne, le remplacement n'est pas effectué plus d'une fois.
Exemple:

``` sql
SELECT replaceRegexpAll('Hello, World!', '^', 'here: ') AS res
```

``` text
┌─res─────────────────┐
│ here: Hello, World! │
└─────────────────────┘
```

## regexpQuoteMeta (s) {#regexpquotemetas}

La fonction ajoute une barre oblique inverse avant certains caractères prédéfinis dans la chaîne.
Les personnages prédéfinis: ‘0’, ‘\\’, ‘\|’, ‘(’, ‘)’, ‘^’, ‘$’, ‘.’, ‘\[’, '\]', ‘?’, '\*‘,’+‘,’{‘,’:‘,’-'.
Cette implémentation diffère légèrement de re2:: RE2:: QuoteMeta. Il échappe à zéro octet comme \\0 au lieu de 00 et il échappe uniquement les caractères requis.
Pour plus d'informations, voir le lien: [RE2](https://github.com/google/re2/blob/master/re2/re2.cc#L473)

[Article Original](https://clickhouse.tech/docs/en/query_language/functions/string_replace_functions/) <!--hide-->
