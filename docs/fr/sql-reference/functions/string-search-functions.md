---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 41
toc_title: "Pour Rechercher Des Cha\xEEnes"
---

# Fonctions de recherche de chaînes {#functions-for-searching-strings}

La recherche est sensible à la casse par défaut dans toutes ces fonctions. Il existe des variantes pour la recherche insensible à la casse.

## position(botte de foin, aiguille), localiser( botte de foin, aiguille) {#position}

Renvoie la position (en octets) de la sous-chaîne trouvée dans la chaîne, à partir de 1.

Fonctionne sous l'hypothèse que la chaîne de caractères contient un ensemble d'octets représentant un octet texte codé. Si cette hypothèse n'est pas remplie et qu'un caractère ne peut pas être représenté à l'aide d'un seul octet, la fonction ne lance pas d'exception et renvoie un résultat inattendu. Si le caractère peut être représenté en utilisant deux octets, il utilisera deux octets et ainsi de suite.

Pour une recherche insensible à la casse, utilisez la fonction [positioncaseinsensible](#positioncaseinsensitive).

**Syntaxe**

``` sql
position(haystack, needle)
```

Alias: `locate(haystack, needle)`.

**Paramètre**

-   `haystack` — string, in which substring will to be searched. [Chaîne](../syntax.md#syntax-string-literal).
-   `needle` — substring to be searched. [Chaîne](../syntax.md#syntax-string-literal).

**Valeurs renvoyées**

-   Position de départ en octets (à partir de 1), si la sous-chaîne a été trouvée.
-   0, si la sous-chaîne n'a pas été trouvé.

Type: `Integer`.

**Exemple**

Phrase “Hello, world!” contient un ensemble d'octets représentant un octet texte codé. La fonction renvoie un résultat attendu:

Requête:

``` sql
SELECT position('Hello, world!', '!')
```

Résultat:

``` text
┌─position('Hello, world!', '!')─┐
│                             13 │
└────────────────────────────────┘
```

La même phrase en russe contient des caractères qui ne peuvent pas être représentés en utilisant un seul octet. La fonction renvoie un résultat inattendu (utilisation [positionUTF8](#positionutf8) fonction pour le texte codé sur plusieurs octets):

Requête:

``` sql
SELECT position('Привет, мир!', '!')
```

Résultat:

``` text
┌─position('Привет, мир!', '!')─┐
│                            21 │
└───────────────────────────────┘
```

## positioncaseinsensible {#positioncaseinsensitive}

Le même que [position](#position) renvoie la position (en octets) de la sous-chaîne trouvée dans la chaîne, à partir de 1. Utilisez la fonction pour une recherche insensible à la casse.

Fonctionne sous l'hypothèse que la chaîne de caractères contient un ensemble d'octets représentant un octet texte codé. Si cette hypothèse n'est pas remplie et qu'un caractère ne peut pas être représenté à l'aide d'un seul octet, la fonction ne lance pas d'exception et renvoie un résultat inattendu. Si le caractère peut être représenté en utilisant deux octets, il utilisera deux octets et ainsi de suite.

**Syntaxe**

``` sql
positionCaseInsensitive(haystack, needle)
```

**Paramètre**

-   `haystack` — string, in which substring will to be searched. [Chaîne](../syntax.md#syntax-string-literal).
-   `needle` — substring to be searched. [Chaîne](../syntax.md#syntax-string-literal).

**Valeurs renvoyées**

-   Position de départ en octets (à partir de 1), si la sous-chaîne a été trouvée.
-   0, si la sous-chaîne n'a pas été trouvé.

Type: `Integer`.

**Exemple**

Requête:

``` sql
SELECT positionCaseInsensitive('Hello, world!', 'hello')
```

Résultat:

``` text
┌─positionCaseInsensitive('Hello, world!', 'hello')─┐
│                                                 1 │
└───────────────────────────────────────────────────┘
```

## positionUTF8 {#positionutf8}

Renvoie la position (en points Unicode) de la sous-chaîne trouvée dans la chaîne, à partir de 1.

Fonctionne sous l'hypothèse que la chaîne contient un ensemble d'octets représentant un texte codé en UTF-8. Si cette hypothèse n'est pas remplie, la fonction ne lance pas d'exception et renvoie un résultat inattendu. Si le caractère peut être représenté en utilisant deux points Unicode, il en utilisera deux et ainsi de suite.

Pour une recherche insensible à la casse, utilisez la fonction [positionCaseInsensitiveUTF8](#positioncaseinsensitiveutf8).

**Syntaxe**

``` sql
positionUTF8(haystack, needle)
```

**Paramètre**

-   `haystack` — string, in which substring will to be searched. [Chaîne](../syntax.md#syntax-string-literal).
-   `needle` — substring to be searched. [Chaîne](../syntax.md#syntax-string-literal).

**Valeurs renvoyées**

-   Position de départ dans les points Unicode (à partir de 1), si la sous-chaîne a été trouvée.
-   0, si la sous-chaîne n'a pas été trouvé.

Type: `Integer`.

**Exemple**

Phrase “Hello, world!” en russe contient un ensemble de points Unicode représentant un texte codé à un seul point. La fonction renvoie un résultat attendu:

Requête:

``` sql
SELECT positionUTF8('Привет, мир!', '!')
```

Résultat:

``` text
┌─positionUTF8('Привет, мир!', '!')─┐
│                                12 │
└───────────────────────────────────┘
```

Phrase “Salut, étudiante!” où le caractère `é` peut être représenté en utilisant un point (`U+00E9`) ou deux points (`U+0065U+0301`) la fonction peut être retournée un résultat inattendu:

Requête pour la lettre `é` qui est représenté un point Unicode `U+00E9`:

``` sql
SELECT positionUTF8('Salut, étudiante!', '!')
```

Résultat:

``` text
┌─positionUTF8('Salut, étudiante!', '!')─┐
│                                     17 │
└────────────────────────────────────────┘
```

Requête pour la lettre `é` qui est représenté deux points Unicode `U+0065U+0301`:

``` sql
SELECT positionUTF8('Salut, étudiante!', '!')
```

Résultat:

``` text
┌─positionUTF8('Salut, étudiante!', '!')─┐
│                                     18 │
└────────────────────────────────────────┘
```

## positionCaseInsensitiveUTF8 {#positioncaseinsensitiveutf8}

Le même que [positionUTF8](#positionutf8) mais est sensible à la casse. Renvoie la position (en points Unicode) de la sous-chaîne trouvée dans la chaîne, à partir de 1.

Fonctionne sous l'hypothèse que la chaîne contient un ensemble d'octets représentant un texte codé en UTF-8. Si cette hypothèse n'est pas remplie, la fonction ne lance pas d'exception et renvoie un résultat inattendu. Si le caractère peut être représenté en utilisant deux points Unicode, il en utilisera deux et ainsi de suite.

**Syntaxe**

``` sql
positionCaseInsensitiveUTF8(haystack, needle)
```

**Paramètre**

-   `haystack` — string, in which substring will to be searched. [Chaîne](../syntax.md#syntax-string-literal).
-   `needle` — substring to be searched. [Chaîne](../syntax.md#syntax-string-literal).

**Valeur renvoyée**

-   Position de départ dans les points Unicode (à partir de 1), si la sous-chaîne a été trouvée.
-   0, si la sous-chaîne n'a pas été trouvé.

Type: `Integer`.

**Exemple**

Requête:

``` sql
SELECT positionCaseInsensitiveUTF8('Привет, мир!', 'Мир')
```

Résultat:

``` text
┌─positionCaseInsensitiveUTF8('Привет, мир!', 'Мир')─┐
│                                                  9 │
└────────────────────────────────────────────────────┘
```

## multirecherchallpositions {#multisearchallpositions}

Le même que [position](string-search-functions.md#position) mais les retours `Array` des positions (en octets) des sous-chaînes correspondantes trouvées dans la chaîne. Les Positions sont indexées à partir de 1.

La recherche est effectuée sur des séquences d'octets sans tenir compte de l'encodage et du classement des chaînes.

-   Pour une recherche ASCII insensible à la casse, utilisez la fonction `multiSearchAllPositionsCaseInsensitive`.
-   Pour la recherche en UTF-8, Utilisez la fonction [multiSearchAllPositionsUTF8](#multiSearchAllPositionsUTF8).
-   Pour la recherche UTF-8 insensible à la casse, utilisez la fonction multiSearchAllPositionsCaseInsensitiveutf8.

**Syntaxe**

``` sql
multiSearchAllPositions(haystack, [needle1, needle2, ..., needlen])
```

**Paramètre**

-   `haystack` — string, in which substring will to be searched. [Chaîne](../syntax.md#syntax-string-literal).
-   `needle` — substring to be searched. [Chaîne](../syntax.md#syntax-string-literal).

**Valeurs renvoyées**

-   Tableau de positions de départ en octets (à partir de 1), si la sous-chaîne correspondante a été trouvée et 0 si elle n'est pas trouvée.

**Exemple**

Requête:

``` sql
SELECT multiSearchAllPositions('Hello, World!', ['hello', '!', 'world'])
```

Résultat:

``` text
┌─multiSearchAllPositions('Hello, World!', ['hello', '!', 'world'])─┐
│ [0,13,0]                                                          │
└───────────────────────────────────────────────────────────────────┘
```

## multiSearchAllPositionsUTF8 {#multiSearchAllPositionsUTF8}

Voir `multiSearchAllPositions`.

## multiSearchFirstPosition(botte de foin, \[aiguille<sub>1</sub>, aiguille<sub>2</sub>, …, needle<sub>et</sub>\]) {#multisearchfirstposition}

Le même que `position` mais renvoie le décalage le plus à gauche de la chaîne `haystack` et qui correspond à certains des aiguilles.

Pour une recherche insensible à la casse ou/et au format UTF-8, utilisez les fonctions `multiSearchFirstPositionCaseInsensitive, multiSearchFirstPositionUTF8, multiSearchFirstPositionCaseInsensitiveUTF8`.

## multiSearchFirstIndex(botte de foin, \[aiguille<sub>1</sub>, aiguille<sub>2</sub>, …, needle<sub>et</sub>\]) {#multisearchfirstindexhaystack-needle1-needle2-needlen}

Renvoie l'index `i` (à partir de 1) de l'aiguille trouvée la plus à gauche<sub>je</sub> dans la chaîne `haystack` et 0 sinon.

Pour une recherche insensible à la casse ou/et au format UTF-8, utilisez les fonctions `multiSearchFirstIndexCaseInsensitive, multiSearchFirstIndexUTF8, multiSearchFirstIndexCaseInsensitiveUTF8`.

## multiSearchAny(botte de foin, \[aiguille<sub>1</sub>, aiguille<sub>2</sub>, …, needle<sub>et</sub>\]) {#function-multisearchany}

Renvoie 1, si au moins une aiguille de chaîne<sub>je</sub> correspond à la chaîne `haystack` et 0 sinon.

Pour une recherche insensible à la casse ou/et au format UTF-8, utilisez les fonctions `multiSearchAnyCaseInsensitive, multiSearchAnyUTF8, multiSearchAnyCaseInsensitiveUTF8`.

!!! note "Note"
    Dans tous les `multiSearch*` fonctions le nombre d'aiguilles doit être d'au moins 2<sup>8</sup> en raison de la spécification de mise en œuvre.

## match (botte de foin, motif) {#matchhaystack-pattern}

Vérifie si la chaîne correspond au `pattern` expression régulière. Un `re2` expression régulière. Le [syntaxe](https://github.com/google/re2/wiki/Syntax) de la `re2` les expressions régulières sont plus limitées que la syntaxe des expressions régulières Perl.

Renvoie 0 si elle ne correspond pas, ou 1 si elle correspond.

Notez que le symbole antislash (`\`) est utilisé pour s'échapper dans l'expression régulière. Le même symbole est utilisé pour échapper dans les littéraux de chaîne. Donc, pour échapper au symbole dans une expression régulière, vous devez écrire deux barres obliques inverses ( \\ ) dans un littéral de chaîne.

L'expression régulière travaille à la chaîne, comme si c'est un ensemble d'octets. L'expression régulière ne peut pas contenir d'octets nuls.
Pour que les modèles recherchent des sous-chaînes dans une chaîne, il est préférable D'utiliser LIKE ou ‘position’ depuis ils travaillent beaucoup plus vite.

## multiMatchAny(botte de foin, \[motif<sub>1</sub>, modèle<sub>2</sub>, …, pattern<sub>et</sub>\]) {#multimatchanyhaystack-pattern1-pattern2-patternn}

Le même que `match` mais renvoie 0 si aucune des expressions régulières sont appariés et 1 si l'un des modèles les matchs. Il utilise [hyperscan](https://github.com/intel/hyperscan) bibliothèque. Pour que les modèles recherchent des sous-chaînes dans une chaîne, il est préférable d'utiliser `multiSearchAny` comme cela fonctionne beaucoup plus vite.

!!! note "Note"
    La longueur de l'un des `haystack` la chaîne doit être inférieure à 2<sup>32</sup> octets sinon l'exception est levée. Cette restriction a lieu en raison de l'API hyperscan.

## multiMatchAnyIndex(botte de foin, \[motif<sub>1</sub>, modèle<sub>2</sub>, …, pattern<sub>et</sub>\]) {#multimatchanyindexhaystack-pattern1-pattern2-patternn}

Le même que `multiMatchAny` mais retourne un index qui correspond à la botte de foin.

## multiMatchAllIndices(botte de foin, \[motif<sub>1</sub>, modèle<sub>2</sub>, …, pattern<sub>et</sub>\]) {#multimatchallindiceshaystack-pattern1-pattern2-patternn}

Le même que `multiMatchAny`, mais renvoie le tableau de tous les indices qui correspondent à la botte de foin dans n'importe quel ordre.

## multiFuzzyMatchAny(botte de foin, distance, \[motif<sub>1</sub>, modèle<sub>2</sub>, …, pattern<sub>et</sub>\]) {#multifuzzymatchanyhaystack-distance-pattern1-pattern2-patternn}

Le même que `multiMatchAny`, mais renvoie 1 si un motif correspond à la botte de foin dans une constante [distance d'édition](https://en.wikipedia.org/wiki/Edit_distance). Cette fonction est également en mode expérimental et peut être extrêmement lente. Pour plus d'informations, voir [documentation hyperscan](https://intel.github.io/hyperscan/dev-reference/compilation.html#approximate-matching).

## multiFuzzyMatchAnyIndex(botte de foin, distance, \[motif<sub>1</sub>, modèle<sub>2</sub>, …, pattern<sub>et</sub>\]) {#multifuzzymatchanyindexhaystack-distance-pattern1-pattern2-patternn}

Le même que `multiFuzzyMatchAny`, mais renvoie tout index qui correspond à la botte de foin à une distance d'édition constante.

## multiFuzzyMatchAllIndices(botte de foin, distance, \[motif<sub>1</sub>, modèle<sub>2</sub>, …, pattern<sub>et</sub>\]) {#multifuzzymatchallindiceshaystack-distance-pattern1-pattern2-patternn}

Le même que `multiFuzzyMatchAny`, mais renvoie le tableau de tous les indices dans n'importe quel ordre qui correspond à la botte de foin à une distance d'édition constante.

!!! note "Note"
    `multiFuzzyMatch*` les fonctions ne prennent pas en charge les expressions régulières UTF-8, et ces expressions sont traitées comme des octets en raison de la restriction hyperscan.

!!! note "Note"
    Pour désactiver toutes les fonctions qui utilisent hyperscan, utilisez le réglage `SET allow_hyperscan = 0;`.

## extrait(botte de foin, motif) {#extracthaystack-pattern}

Extraits d'un fragment d'une chaîne à l'aide d'une expression régulière. Si ‘haystack’ ne correspond pas à l' ‘pattern’ regex, une chaîne vide est renvoyée. Si l'expression rationnelle ne contient pas de sous-modèles, elle prend le fragment qui correspond à l'expression rationnelle entière. Sinon, il prend le fragment qui correspond au premier sous-masque.

## extractAll(botte de foin, motif) {#extractallhaystack-pattern}

Extrait tous les fragments d'une chaîne à l'aide d'une expression régulière. Si ‘haystack’ ne correspond pas à l' ‘pattern’ regex, une chaîne vide est renvoyée. Renvoie un tableau de chaînes composé de toutes les correspondances à l'expression rationnelle. En général, le comportement est le même que le ‘extract’ fonction (il prend le premier sous-masque, ou l'expression entière s'il n'y a pas de sous-masque).

## comme (botte de foin, motif), botte de foin comme opérateur de motif {#function-like}

Vérifie si une chaîne correspond à une expression régulière simple.
L'expression régulière peut contenir les métasymboles `%` et `_`.

`%` indique n'importe quelle quantité d'octets (y compris zéro caractère).

`_` indique un octet.

Utilisez la barre oblique inverse (`\`) pour échapper aux métasymboles. Voir la note sur l'échappement dans la description du ‘match’ fonction.

Pour les expressions régulières comme `%needle%`, le code est plus optimale et fonctionne aussi vite que le `position` fonction.
Pour d'autres expressions régulières, le code est le même que pour la ‘match’ fonction.

## notLike (botte de foin, motif), botte de foin pas comme opérateur de motif {#function-notlike}

La même chose que ‘like’ mais négative.

## ngramDistance(botte de foin, aiguille) {#ngramdistancehaystack-needle}

Calcule la distance de 4 grammes entre `haystack` et `needle`: counts the symmetric difference between two multisets of 4-grams and normalizes it by the sum of their cardinalities. Returns float number from 0 to 1 – the closer to zero, the more strings are similar to each other. If the constant `needle` ou `haystack` est plus de 32Kb, jette une exception. Si une partie de la non-constante `haystack` ou `needle` les chaînes sont plus que 32Kb, la distance est toujours un.

Pour une recherche insensible à la casse ou/et au format UTF-8, utilisez les fonctions `ngramDistanceCaseInsensitive, ngramDistanceUTF8, ngramDistanceCaseInsensitiveUTF8`.

## ngramSearch(botte de foin, aiguille) {#ngramsearchhaystack-needle}

Même que `ngramDistance` mais calcule la différence non symétrique entre `needle` et `haystack` – the number of n-grams from needle minus the common number of n-grams normalized by the number of `needle` n-grammes. Le plus proche d'un, le plus probable `needle` est dans le `haystack`. Peut être utile pour la recherche de chaîne floue.

Pour une recherche insensible à la casse ou/et au format UTF-8, utilisez les fonctions `ngramSearchCaseInsensitive, ngramSearchUTF8, ngramSearchCaseInsensitiveUTF8`.

!!! note "Note"
    For UTF-8 case we use 3-gram distance. All these are not perfectly fair n-gram distances. We use 2-byte hashes to hash n-grams and then calculate the (non-)symmetric difference between these hash tables – collisions may occur. With UTF-8 case-insensitive format we do not use fair `tolower` function – we zero the 5-th bit (starting from zero) of each codepoint byte and first bit of zeroth byte if bytes more than one – this works for Latin and mostly for all Cyrillic letters.

[Article Original](https://clickhouse.tech/docs/en/query_language/functions/string_search_functions/) <!--hide-->
