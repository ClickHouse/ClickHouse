---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 66
toc_title: Autre
---

# D'Autres Fonctions {#other-functions}

## hôte() {#hostname}

Renvoie une chaîne avec le nom de l'hôte sur lequel cette fonction a été exécutée. Pour le traitement distribué, c'est le nom du serveur distant, si la fonction est exécutée sur un serveur distant.

## getMacro {#getmacro}

Obtient une valeur nommée à partir [macro](../../operations/server-configuration-parameters/settings.md#macros) la section de la configuration du serveur.

**Syntaxe**

``` sql
getMacro(name);
```

**Paramètre**

-   `name` — Name to retrieve from the `macros` section. [Chaîne](../../sql-reference/data-types/string.md#string).

**Valeur renvoyée**

-   Valeur de la macro spécifiée.

Type: [Chaîne](../../sql-reference/data-types/string.md).

**Exemple**

Exemple `macros` section dans le fichier de configuration du serveur:

``` xml
<macros>
    <test>Value</test>
</macros>
```

Requête:

``` sql
SELECT getMacro('test');
```

Résultat:

``` text
┌─getMacro('test')─┐
│ Value            │
└──────────────────┘
```

Une méthode alternative pour obtenir la même valeur:

``` sql
SELECT * FROM system.macros
WHERE macro = 'test';
```

``` text
┌─macro─┬─substitution─┐
│ test  │ Value        │
└───────┴──────────────┘
```

## FQDN {#fqdn}

Retourne le nom de domaine pleinement qualifié.

**Syntaxe**

``` sql
fqdn();
```

Cette fonction est insensible à la casse.

**Valeur renvoyée**

-   Chaîne avec le nom de domaine complet.

Type: `String`.

**Exemple**

Requête:

``` sql
SELECT FQDN();
```

Résultat:

``` text
┌─FQDN()──────────────────────────┐
│ clickhouse.ru-central1.internal │
└─────────────────────────────────┘
```

## basename {#basename}

Extrait la partie finale d'une chaîne après la dernière barre oblique ou barre oblique inverse. Cette fonction est souvent utilisée pour extraire le nom de fichier d'un chemin.

``` sql
basename( expr )
```

**Paramètre**

-   `expr` — Expression resulting in a [Chaîne](../../sql-reference/data-types/string.md) type de valeur. Tous les antislashs doivent être échappés dans la valeur résultante.

**Valeur Renvoyée**

Une chaîne de caractères qui contient:

-   La partie finale d'une chaîne après la dernière barre oblique ou barre oblique inverse.

        If the input string contains a path ending with slash or backslash, for example, `/` or `c:\`, the function returns an empty string.

-   La chaîne d'origine s'il n'y a pas de barres obliques ou de barres obliques inverses.

**Exemple**

``` sql
SELECT 'some/long/path/to/file' AS a, basename(a)
```

``` text
┌─a──────────────────────┬─basename('some\\long\\path\\to\\file')─┐
│ some\long\path\to\file │ file                                   │
└────────────────────────┴────────────────────────────────────────┘
```

``` sql
SELECT 'some\\long\\path\\to\\file' AS a, basename(a)
```

``` text
┌─a──────────────────────┬─basename('some\\long\\path\\to\\file')─┐
│ some\long\path\to\file │ file                                   │
└────────────────────────┴────────────────────────────────────────┘
```

``` sql
SELECT 'some-file-name' AS a, basename(a)
```

``` text
┌─a──────────────┬─basename('some-file-name')─┐
│ some-file-name │ some-file-name             │
└────────────────┴────────────────────────────┘
```

## visibleWidth (x) {#visiblewidthx}

Calcule la largeur approximative lors de la sortie des valeurs vers la console au format texte (séparé par des tabulations).
Cette fonction est utilisée par le système pour implémenter de jolis formats.

`NULL` est représenté comme une chaîne correspondant à `NULL` dans `Pretty` format.

``` sql
SELECT visibleWidth(NULL)
```

``` text
┌─visibleWidth(NULL)─┐
│                  4 │
└────────────────────┘
```

## toTypeName (x) {#totypenamex}

Renvoie une chaîne contenant le nom du type de l'argument passé.

Si `NULL` est passé à la fonction en entrée, puis il renvoie le `Nullable(Nothing)` type, ce qui correspond à un interne `NULL` représentation à ClickHouse.

## la taille de bloc() {#function-blocksize}

Récupère la taille du bloc.
Dans ClickHouse, les requêtes sont toujours exécutées sur des blocs (ensembles de parties de colonne). Cette fonction permet d'obtenir la taille du bloc pour lequel vous l'avez appelé.

## matérialiser (x) {#materializex}

Transforme une constante dans une colonne contenant une seule valeur.
Dans ClickHouse, les colonnes complètes et les constantes sont représentées différemment en mémoire. Les fonctions fonctionnent différemment pour les arguments constants et les arguments normaux (un code différent est exécuté), bien que le résultat soit presque toujours le même. Cette fonction sert à déboguer ce comportement.

## ignore(…) {#ignore}

Accepte tous les arguments, y compris `NULL`. Renvoie toujours 0.
Cependant, l'argument est toujours évalué. Cela peut être utilisé pour les benchmarks.

## sommeil(secondes) {#sleepseconds}

Dormir ‘seconds’ secondes sur chaque bloc de données. Vous pouvez spécifier un nombre entier ou un nombre à virgule flottante.

## sleepEachRow (secondes) {#sleepeachrowseconds}

Dormir ‘seconds’ secondes sur chaque ligne. Vous pouvez spécifier un nombre entier ou un nombre à virgule flottante.

## currentDatabase() {#currentdatabase}

Retourne le nom de la base de données actuelle.
Vous pouvez utiliser cette fonction dans les paramètres du moteur de table dans une requête CREATE TABLE où vous devez spécifier la base de données.

## currentUser() {#other-function-currentuser}

Renvoie la connexion de l'utilisateur actuel. La connexion de l'utilisateur, cette requête initiée, sera renvoyée en cas de requête distibuted.

``` sql
SELECT currentUser();
```

Alias: `user()`, `USER()`.

**Valeurs renvoyées**

-   Connexion de l'utilisateur actuel.
-   Connexion de l'utilisateur qui a lancé la requête en cas de requête distribuée.

Type: `String`.

**Exemple**

Requête:

``` sql
SELECT currentUser();
```

Résultat:

``` text
┌─currentUser()─┐
│ default       │
└───────────────┘
```

## isConstant {#is-constant}

Vérifie si l'argument est une expression constante.

A constant expression means an expression whose resulting value is known at the query analysis (i.e. before execution). For example, expressions over [littéral](../syntax.md#literals) sont des expressions constantes.

La fonction est destinée au développement, au débogage et à la démonstration.

**Syntaxe**

``` sql
isConstant(x)
```

**Paramètre**

-   `x` — Expression to check.

**Valeurs renvoyées**

-   `1` — `x` est constante.
-   `0` — `x` est non constante.

Type: [UInt8](../data-types/int-uint.md).

**Exemple**

Requête:

``` sql
SELECT isConstant(x + 1) FROM (SELECT 43 AS x)
```

Résultat:

``` text
┌─isConstant(plus(x, 1))─┐
│                      1 │
└────────────────────────┘
```

Requête:

``` sql
WITH 3.14 AS pi SELECT isConstant(cos(pi))
```

Résultat:

``` text
┌─isConstant(cos(pi))─┐
│                   1 │
└─────────────────────┘
```

Requête:

``` sql
SELECT isConstant(number) FROM numbers(1)
```

Résultat:

``` text
┌─isConstant(number)─┐
│                  0 │
└────────────────────┘
```

## isFinite (x) {#isfinitex}

Accepte Float32 et Float64 et renvoie UInt8 égal à 1 si l'argument n'est pas infini et pas un NaN, sinon 0.

## isInfinite (x) {#isinfinitex}

Accepte Float32 et Float64 et renvoie UInt8 égal à 1 si l'argument est infini, sinon 0. Notez que 0 est retourné pour un NaN.

## ifNotFinite {#ifnotfinite}

Vérifie si la valeur à virgule flottante est finie.

**Syntaxe**

    ifNotFinite(x,y)

**Paramètre**

-   `x` — Value to be checked for infinity. Type: [Flottant\*](../../sql-reference/data-types/float.md).
-   `y` — Fallback value. Type: [Flottant\*](../../sql-reference/data-types/float.md).

**Valeur renvoyée**

-   `x` si `x` est finie.
-   `y` si `x` n'est pas finie.

**Exemple**

Requête:

    SELECT 1/0 as infimum, ifNotFinite(infimum,42)

Résultat:

    ┌─infimum─┬─ifNotFinite(divide(1, 0), 42)─┐
    │     inf │                            42 │
    └─────────┴───────────────────────────────┘

Vous pouvez obtenir un résultat similaire en utilisant [opérateur ternaire](conditional-functions.md#ternary-operator): `isFinite(x) ? x : y`.

## isNaN (x) {#isnanx}

Accepte Float32 et Float64 et renvoie UInt8 égal à 1 si l'argument est un NaN, sinon 0.

## hasColumnInTable(\[‘hostname’\[, ‘username’\[, ‘password’\]\],\] ‘database’, ‘table’, ‘column’) {#hascolumnintablehostname-username-password-database-table-column}

Accepte les chaînes constantes: nom de la base de données, nom de la table et nom de la colonne. Renvoie une expression constante UInt8 égale à 1 s'il y a une colonne, sinon 0. Si le paramètre hostname est défini, le test s'exécutera sur un serveur distant.
La fonction renvoie une exception si la table n'existe pas.
Pour les éléments imbriqués structure des données, la fonction vérifie l'existence d'une colonne. Pour la structure de données imbriquée elle-même, la fonction renvoie 0.

## bar {#function-bar}

Permet de construire un diagramme unicode-art.

`bar(x, min, max, width)` dessine une bande avec une largeur proportionnelle à `(x - min)` et égale à `width` les caractères lors de la `x = max`.

Paramètre:

-   `x` — Size to display.
-   `min, max` — Integer constants. The value must fit in `Int64`.
-   `width` — Constant, positive integer, can be fractional.

La bande dessinée avec précision à un huitième d'un symbole.

Exemple:

``` sql
SELECT
    toHour(EventTime) AS h,
    count() AS c,
    bar(c, 0, 600000, 20) AS bar
FROM test.hits
GROUP BY h
ORDER BY h ASC
```

``` text
┌──h─┬──────c─┬─bar────────────────┐
│  0 │ 292907 │ █████████▋         │
│  1 │ 180563 │ ██████             │
│  2 │ 114861 │ ███▋               │
│  3 │  85069 │ ██▋                │
│  4 │  68543 │ ██▎                │
│  5 │  78116 │ ██▌                │
│  6 │ 113474 │ ███▋               │
│  7 │ 170678 │ █████▋             │
│  8 │ 278380 │ █████████▎         │
│  9 │ 391053 │ █████████████      │
│ 10 │ 457681 │ ███████████████▎   │
│ 11 │ 493667 │ ████████████████▍  │
│ 12 │ 509641 │ ████████████████▊  │
│ 13 │ 522947 │ █████████████████▍ │
│ 14 │ 539954 │ █████████████████▊ │
│ 15 │ 528460 │ █████████████████▌ │
│ 16 │ 539201 │ █████████████████▊ │
│ 17 │ 523539 │ █████████████████▍ │
│ 18 │ 506467 │ ████████████████▊  │
│ 19 │ 520915 │ █████████████████▎ │
│ 20 │ 521665 │ █████████████████▍ │
│ 21 │ 542078 │ ██████████████████ │
│ 22 │ 493642 │ ████████████████▍  │
│ 23 │ 400397 │ █████████████▎     │
└────┴────────┴────────────────────┘
```

## transformer {#transform}

Transforme une valeur en fonction explicitement définis cartographie de certains éléments à l'autre.
Il existe deux variantes de cette fonction:

### de transformation(x, array\_from, array\_to, par défaut) {#transformx-array-from-array-to-default}

`x` – What to transform.

`array_from` – Constant array of values for converting.

`array_to` – Constant array of values to convert the values in ‘from’ de.

`default` – Which value to use if ‘x’ n'est pas égale à une des valeurs de ‘from’.

`array_from` et `array_to` – Arrays of the same size.

Type:

`transform(T, Array(T), Array(U), U) -> U`

`T` et `U` peuvent être des types numériques, chaîne ou Date ou DateTime.
Lorsque la même lettre est indiquée (T ou U), pour les types numériques, il se peut qu'il ne s'agisse pas de types correspondants, mais de types ayant un type commun.
Par exemple, le premier argument peut avoir le type Int64, tandis que le second a le type Array(UInt16).

Si l' ‘x’ la valeur est égale à l'un des éléments dans la ‘array\_from’ tableau, elle renvoie l'élément existant (qui est numéroté de même) de la ‘array\_to’ tableau. Sinon, elle renvoie ‘default’. S'il y a plusieurs éléments correspondants dans ‘array\_from’ il renvoie l'un des matches.

Exemple:

``` sql
SELECT
    transform(SearchEngineID, [2, 3], ['Yandex', 'Google'], 'Other') AS title,
    count() AS c
FROM test.hits
WHERE SearchEngineID != 0
GROUP BY title
ORDER BY c DESC
```

``` text
┌─title─────┬──────c─┐
│ Yandex    │ 498635 │
│ Google    │ 229872 │
│ Other     │ 104472 │
└───────────┴────────┘
```

### de transformation(x, array\_from, array\_to) {#transformx-array-from-array-to}

Diffère de la première variation en ce que le ‘default’ l'argument est omis.
Si l' ‘x’ la valeur est égale à l'un des éléments dans la ‘array\_from’ tableau, elle renvoie l'élément correspondant (qui est numéroté de même) de la ‘array\_to’ tableau. Sinon, elle renvoie ‘x’.

Type:

`transform(T, Array(T), Array(T)) -> T`

Exemple:

``` sql
SELECT
    transform(domain(Referer), ['yandex.ru', 'google.ru', 'vk.com'], ['www.yandex', 'example.com']) AS s,
    count() AS c
FROM test.hits
GROUP BY domain(Referer)
ORDER BY count() DESC
LIMIT 10
```

``` text
┌─s──────────────┬───────c─┐
│                │ 2906259 │
│ www.yandex     │  867767 │
│ ███████.ru     │  313599 │
│ mail.yandex.ru │  107147 │
│ ██████.ru      │  100355 │
│ █████████.ru   │   65040 │
│ news.yandex.ru │   64515 │
│ ██████.net     │   59141 │
│ example.com    │   57316 │
└────────────────┴─────────┘
```

## formatReadableSize (x) {#formatreadablesizex}

Accepte la taille (nombre d'octets). Renvoie une taille arrondie avec un suffixe (KiB, MiB, etc.) comme une chaîne de caractères.

Exemple:

``` sql
SELECT
    arrayJoin([1, 1024, 1024*1024, 192851925]) AS filesize_bytes,
    formatReadableSize(filesize_bytes) AS filesize
```

``` text
┌─filesize_bytes─┬─filesize───┐
│              1 │ 1.00 B     │
│           1024 │ 1.00 KiB   │
│        1048576 │ 1.00 MiB   │
│      192851925 │ 183.92 MiB │
└────────────────┴────────────┘
```

## moins (a, b) {#leasta-b}

Renvoie la plus petite valeur de a et b.

## la plus grande(a, b) {#greatesta-b}

Renvoie la plus grande valeur de a et B.

## le temps de disponibilité() {#uptime}

Renvoie la disponibilité du serveur en quelques secondes.

## version() {#version}

Renvoie la version du serveur sous forme de chaîne.

## fuseau() {#timezone}

Retourne le fuseau horaire du serveur.

## blockNumber {#blocknumber}

Renvoie le numéro de séquence du bloc de données où se trouve la ligne.

## rowNumberInBlock {#function-rownumberinblock}

Renvoie le numéro de séquence de la ligne dans le bloc de données. Différents blocs de données sont toujours recalculés.

## rowNumberInAllBlocks() {#rownumberinallblocks}

Renvoie le numéro de séquence de la ligne dans le bloc de données. Cette fonction ne prend en compte que les blocs de données affectés.

## voisin {#neighbor}

La fonction de fenêtre qui donne accès à une ligne à un décalage spécifié qui vient avant ou après la ligne actuelle d'une colonne donnée.

**Syntaxe**

``` sql
neighbor(column, offset[, default_value])
```

Le résultat de la fonction dépend du touché des blocs de données et l'ordre des données dans le bloc.
Si vous créez une sous-requête avec ORDER BY et appelez la fonction depuis l'extérieur de la sous-requête, vous pouvez obtenir le résultat attendu.

**Paramètre**

-   `column` — A column name or scalar expression.
-   `offset` — The number of rows forwards or backwards from the current row of `column`. [Int64](../../sql-reference/data-types/int-uint.md).
-   `default_value` — Optional. The value to be returned if offset goes beyond the scope of the block. Type of data blocks affected.

**Valeurs renvoyées**

-   De la valeur pour `column` dans `offset` distance de la ligne actuelle si `offset` la valeur n'est pas en dehors des limites du bloc.
-   La valeur par défaut pour `column` si `offset` la valeur est en dehors des limites du bloc. Si `default_value` est donné, alors il sera utilisé.

Type: type de blocs de données affectés ou type de valeur par défaut.

**Exemple**

Requête:

``` sql
SELECT number, neighbor(number, 2) FROM system.numbers LIMIT 10;
```

Résultat:

``` text
┌─number─┬─neighbor(number, 2)─┐
│      0 │                   2 │
│      1 │                   3 │
│      2 │                   4 │
│      3 │                   5 │
│      4 │                   6 │
│      5 │                   7 │
│      6 │                   8 │
│      7 │                   9 │
│      8 │                   0 │
│      9 │                   0 │
└────────┴─────────────────────┘
```

Requête:

``` sql
SELECT number, neighbor(number, 2, 999) FROM system.numbers LIMIT 10;
```

Résultat:

``` text
┌─number─┬─neighbor(number, 2, 999)─┐
│      0 │                        2 │
│      1 │                        3 │
│      2 │                        4 │
│      3 │                        5 │
│      4 │                        6 │
│      5 │                        7 │
│      6 │                        8 │
│      7 │                        9 │
│      8 │                      999 │
│      9 │                      999 │
└────────┴──────────────────────────┘
```

Cette fonction peut être utilisée pour calculer une année à valeur métrique:

Requête:

``` sql
WITH toDate('2018-01-01') AS start_date
SELECT
    toStartOfMonth(start_date + (number * 32)) AS month,
    toInt32(month) % 100 AS money,
    neighbor(money, -12) AS prev_year,
    round(prev_year / money, 2) AS year_over_year
FROM numbers(16)
```

Résultat:

``` text
┌──────month─┬─money─┬─prev_year─┬─year_over_year─┐
│ 2018-01-01 │    32 │         0 │              0 │
│ 2018-02-01 │    63 │         0 │              0 │
│ 2018-03-01 │    91 │         0 │              0 │
│ 2018-04-01 │    22 │         0 │              0 │
│ 2018-05-01 │    52 │         0 │              0 │
│ 2018-06-01 │    83 │         0 │              0 │
│ 2018-07-01 │    13 │         0 │              0 │
│ 2018-08-01 │    44 │         0 │              0 │
│ 2018-09-01 │    75 │         0 │              0 │
│ 2018-10-01 │     5 │         0 │              0 │
│ 2018-11-01 │    36 │         0 │              0 │
│ 2018-12-01 │    66 │         0 │              0 │
│ 2019-01-01 │    97 │        32 │           0.33 │
│ 2019-02-01 │    28 │        63 │           2.25 │
│ 2019-03-01 │    56 │        91 │           1.62 │
│ 2019-04-01 │    87 │        22 │           0.25 │
└────────────┴───────┴───────────┴────────────────┘
```

## runningDifference(x) {#other_functions-runningdifference}

Calculates the difference between successive row values ​​in the data block.
Renvoie 0 pour la première ligne et la différence par rapport à la rangée précédente pour chaque nouvelle ligne.

Le résultat de la fonction dépend du touché des blocs de données et l'ordre des données dans le bloc.
Si vous créez une sous-requête avec ORDER BY et appelez la fonction depuis l'extérieur de la sous-requête, vous pouvez obtenir le résultat attendu.

Exemple:

``` sql
SELECT
    EventID,
    EventTime,
    runningDifference(EventTime) AS delta
FROM
(
    SELECT
        EventID,
        EventTime
    FROM events
    WHERE EventDate = '2016-11-24'
    ORDER BY EventTime ASC
    LIMIT 5
)
```

``` text
┌─EventID─┬───────────EventTime─┬─delta─┐
│    1106 │ 2016-11-24 00:00:04 │     0 │
│    1107 │ 2016-11-24 00:00:05 │     1 │
│    1108 │ 2016-11-24 00:00:05 │     0 │
│    1109 │ 2016-11-24 00:00:09 │     4 │
│    1110 │ 2016-11-24 00:00:10 │     1 │
└─────────┴─────────────────────┴───────┘
```

Veuillez noter que la taille du bloc affecte le résultat. Avec chaque nouveau bloc, le `runningDifference` l'état est réinitialisé.

``` sql
SELECT
    number,
    runningDifference(number + 1) AS diff
FROM numbers(100000)
WHERE diff != 1
```

``` text
┌─number─┬─diff─┐
│      0 │    0 │
└────────┴──────┘
┌─number─┬─diff─┐
│  65536 │    0 │
└────────┴──────┘
```

``` sql
set max_block_size=100000 -- default value is 65536!

SELECT
    number,
    runningDifference(number + 1) AS diff
FROM numbers(100000)
WHERE diff != 1
```

``` text
┌─number─┬─diff─┐
│      0 │    0 │
└────────┴──────┘
```

## runningDifferenceStartingWithFirstvalue {#runningdifferencestartingwithfirstvalue}

De même que pour [runningDifference](./other-functions.md#other_functions-runningdifference) la différence est la valeur de la première ligne, est retourné à la valeur de la première ligne, et chaque rangée suivante renvoie la différence de la rangée précédente.

## MACNumToString (num) {#macnumtostringnum}

Accepte un numéro UInt64. Interprète comme une adresse MAC dans big endian. Renvoie une chaîne contenant l'adresse MAC correspondante au format AA:BB:CC: DD:EE: FF (Nombres séparés par deux points sous forme hexadécimale).

## MACStringToNum (s) {#macstringtonums}

La fonction inverse de MACNumToString. Si l'adresse MAC a un format non valide, elle renvoie 0.

## MACStringToOUI (s) {#macstringtoouis}

Accepte une adresse MAC au format AA:BB:CC: DD:EE: FF (Nombres séparés par deux points sous forme hexadécimale). Renvoie les trois premiers octets sous la forme D'un nombre UInt64. Si l'adresse MAC a un format non valide, elle renvoie 0.

## getSizeOfEnumType {#getsizeofenumtype}

Retourne le nombre de champs dans [Enum](../../sql-reference/data-types/enum.md).

``` sql
getSizeOfEnumType(value)
```

**Paramètre:**

-   `value` — Value of type `Enum`.

**Valeurs renvoyées**

-   Le nombre de champs avec `Enum` les valeurs d'entrée.
-   Une exception est levée si le type n'est pas `Enum`.

**Exemple**

``` sql
SELECT getSizeOfEnumType( CAST('a' AS Enum8('a' = 1, 'b' = 2) ) ) AS x
```

``` text
┌─x─┐
│ 2 │
└───┘
```

## blockSerializedSize {#blockserializedsize}

Retourne la taille sur le disque (sans tenir compte de la compression).

``` sql
blockSerializedSize(value[, value[, ...]])
```

**Paramètre:**

-   `value` — Any value.

**Valeurs renvoyées**

-   Le nombre d'octets qui seront écrites sur le disque pour le bloc de valeurs (sans compression).

**Exemple**

``` sql
SELECT blockSerializedSize(maxState(1)) as x
```

``` text
┌─x─┐
│ 2 │
└───┘
```

## toColumnTypeName {#tocolumntypename}

Renvoie le nom de la classe qui représente le type de données de la colonne dans la RAM.

``` sql
toColumnTypeName(value)
```

**Paramètre:**

-   `value` — Any type of value.

**Valeurs renvoyées**

-   Une chaîne avec le nom de la classe utilisée pour représenter `value` type de données dans la mémoire RAM.

**Exemple de la différence entre`toTypeName ' and ' toColumnTypeName`**

``` sql
SELECT toTypeName(CAST('2018-01-01 01:02:03' AS DateTime))
```

``` text
┌─toTypeName(CAST('2018-01-01 01:02:03', 'DateTime'))─┐
│ DateTime                                            │
└─────────────────────────────────────────────────────┘
```

``` sql
SELECT toColumnTypeName(CAST('2018-01-01 01:02:03' AS DateTime))
```

``` text
┌─toColumnTypeName(CAST('2018-01-01 01:02:03', 'DateTime'))─┐
│ Const(UInt32)                                             │
└───────────────────────────────────────────────────────────┘
```

L'exemple montre que le `DateTime` type de données est stocké dans la mémoire comme `Const(UInt32)`.

## dumpColumnStructure {#dumpcolumnstructure}

Affiche une description détaillée des structures de données en RAM

``` sql
dumpColumnStructure(value)
```

**Paramètre:**

-   `value` — Any type of value.

**Valeurs renvoyées**

-   Une chaîne décrivant la structure utilisée pour représenter `value` type de données dans la mémoire RAM.

**Exemple**

``` sql
SELECT dumpColumnStructure(CAST('2018-01-01 01:02:03', 'DateTime'))
```

``` text
┌─dumpColumnStructure(CAST('2018-01-01 01:02:03', 'DateTime'))─┐
│ DateTime, Const(size = 1, UInt32(size = 1))                  │
└──────────────────────────────────────────────────────────────┘
```

## defaultValueOfArgumentType {#defaultvalueofargumenttype}

Affiche la valeur par défaut du type de données.

Ne pas inclure des valeurs par défaut pour les colonnes personnalisées définies par l'utilisateur.

``` sql
defaultValueOfArgumentType(expression)
```

**Paramètre:**

-   `expression` — Arbitrary type of value or an expression that results in a value of an arbitrary type.

**Valeurs renvoyées**

-   `0` pour les nombres.
-   Chaîne vide pour les chaînes.
-   `ᴺᵁᴸᴸ` pour [Nullable](../../sql-reference/data-types/nullable.md).

**Exemple**

``` sql
SELECT defaultValueOfArgumentType( CAST(1 AS Int8) )
```

``` text
┌─defaultValueOfArgumentType(CAST(1, 'Int8'))─┐
│                                           0 │
└─────────────────────────────────────────────┘
```

``` sql
SELECT defaultValueOfArgumentType( CAST(1 AS Nullable(Int8) ) )
```

``` text
┌─defaultValueOfArgumentType(CAST(1, 'Nullable(Int8)'))─┐
│                                                  ᴺᵁᴸᴸ │
└───────────────────────────────────────────────────────┘
```

## reproduire {#other-functions-replicate}

Crée un tableau avec une seule valeur.

Utilisé pour la mise en œuvre interne de [arrayJoin](array-join.md#functions_arrayjoin).

``` sql
SELECT replicate(x, arr);
```

**Paramètre:**

-   `arr` — Original array. ClickHouse creates a new array of the same length as the original and fills it with the value `x`.
-   `x` — The value that the resulting array will be filled with.

**Valeur renvoyée**

Un tableau rempli de la valeur `x`.

Type: `Array`.

**Exemple**

Requête:

``` sql
SELECT replicate(1, ['a', 'b', 'c'])
```

Résultat:

``` text
┌─replicate(1, ['a', 'b', 'c'])─┐
│ [1,1,1]                       │
└───────────────────────────────┘
```

## filesystemAvailable {#filesystemavailable}

Renvoie la quantité d'espace restant sur le système de fichiers où se trouvent les fichiers des bases de données. Il est toujours plus petit que l'espace libre total ([filesystemFree](#filesystemfree)) parce qu'un peu d'espace est réservé au système D'exploitation.

**Syntaxe**

``` sql
filesystemAvailable()
```

**Valeur renvoyée**

-   La quantité d'espace restant disponible en octets.

Type: [UInt64](../../sql-reference/data-types/int-uint.md).

**Exemple**

Requête:

``` sql
SELECT formatReadableSize(filesystemAvailable()) AS "Available space", toTypeName(filesystemAvailable()) AS "Type";
```

Résultat:

``` text
┌─Available space─┬─Type───┐
│ 30.75 GiB       │ UInt64 │
└─────────────────┴────────┘
```

## filesystemFree {#filesystemfree}

Retourne montant total de l'espace libre sur le système de fichiers où les fichiers des bases de données. Voir aussi `filesystemAvailable`

**Syntaxe**

``` sql
filesystemFree()
```

**Valeur renvoyée**

-   Quantité d'espace libre en octets.

Type: [UInt64](../../sql-reference/data-types/int-uint.md).

**Exemple**

Requête:

``` sql
SELECT formatReadableSize(filesystemFree()) AS "Free space", toTypeName(filesystemFree()) AS "Type";
```

Résultat:

``` text
┌─Free space─┬─Type───┐
│ 32.39 GiB  │ UInt64 │
└────────────┴────────┘
```

## filesystemCapacity {#filesystemcapacity}

Renvoie la capacité du système de fichiers en octets. Pour l'évaluation, la [chemin](../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-path) le répertoire de données doit être configuré.

**Syntaxe**

``` sql
filesystemCapacity()
```

**Valeur renvoyée**

-   Informations de capacité du système de fichiers en octets.

Type: [UInt64](../../sql-reference/data-types/int-uint.md).

**Exemple**

Requête:

``` sql
SELECT formatReadableSize(filesystemCapacity()) AS "Capacity", toTypeName(filesystemCapacity()) AS "Type"
```

Résultat:

``` text
┌─Capacity──┬─Type───┐
│ 39.32 GiB │ UInt64 │
└───────────┴────────┘
```

## finalizeAggregation {#function-finalizeaggregation}

Prend de l'état de la fonction d'agrégation. Renvoie le résultat de l'agrégation (état finalisé).

## runningAccumulate {#function-runningaccumulate}

Prend les membres de la fonction d'agrégation et renvoie une colonne avec des valeurs, sont le résultat de l'accumulation de ces états pour un ensemble de bloc de lignes, de la première à la ligne actuelle.
Par exemple, prend l'état de la fonction d'agrégat (exemple runningAccumulate(uniqState(UserID))), et pour chaque ligne de bloc, retourne le résultat de la fonction d'agrégat lors de la fusion des états de toutes les lignes précédentes et de la ligne actuelle.
Ainsi, le résultat de la fonction dépend de la partition des données aux blocs et de l'ordre des données dans le bloc.

## joinGet {#joinget}

La fonction vous permet d'extraire les données de la table de la même manière qu'à partir d'un [dictionnaire](../../sql-reference/dictionaries/index.md).

Obtient les données de [Rejoindre](../../engines/table-engines/special/join.md#creating-a-table) tables utilisant la clé de jointure spécifiée.

Ne prend en charge que les tables créées avec `ENGINE = Join(ANY, LEFT, <join_keys>)` déclaration.

**Syntaxe**

``` sql
joinGet(join_storage_table_name, `value_column`, join_keys)
```

**Paramètre**

-   `join_storage_table_name` — an [identificateur](../syntax.md#syntax-identifiers) indique l'endroit où la recherche est effectuée. L'identificateur est recherché dans la base de données par défaut (voir paramètre `default_database` dans le fichier de config). Pour remplacer la base de données par défaut, utilisez `USE db_name` ou spécifiez la base de données et la table via le séparateur `db_name.db_table` voir l'exemple.
-   `value_column` — name of the column of the table that contains required data.
-   `join_keys` — list of keys.

**Valeur renvoyée**

Retourne la liste des valeurs correspond à la liste des clés.

Si certain n'existe pas dans la table source alors `0` ou `null` seront renvoyés basé sur [join\_use\_nulls](../../operations/settings/settings.md#join_use_nulls) paramètre.

Plus d'infos sur `join_use_nulls` dans [Opération de jointure](../../engines/table-engines/special/join.md).

**Exemple**

Table d'entrée:

``` sql
CREATE DATABASE db_test
CREATE TABLE db_test.id_val(`id` UInt32, `val` UInt32) ENGINE = Join(ANY, LEFT, id) SETTINGS join_use_nulls = 1
INSERT INTO db_test.id_val VALUES (1,11)(2,12)(4,13)
```

``` text
┌─id─┬─val─┐
│  4 │  13 │
│  2 │  12 │
│  1 │  11 │
└────┴─────┘
```

Requête:

``` sql
SELECT joinGet(db_test.id_val,'val',toUInt32(number)) from numbers(4) SETTINGS join_use_nulls = 1
```

Résultat:

``` text
┌─joinGet(db_test.id_val, 'val', toUInt32(number))─┐
│                                                0 │
│                                               11 │
│                                               12 │
│                                                0 │
└──────────────────────────────────────────────────┘
```

## modelEvaluate(model\_name, …) {#function-modelevaluate}

Évaluer le modèle externe.
Accepte un nom de modèle et le modèle de l'argumentation. Renvoie Float64.

## throwIf (x \[, custom\_message\]) {#throwifx-custom-message}

Lever une exception si l'argument est non nul.
custom\_message - est un paramètre optionnel: une chaîne constante, fournit un message d'erreur

``` sql
SELECT throwIf(number = 3, 'Too many') FROM numbers(10);
```

``` text
↙ Progress: 0.00 rows, 0.00 B (0.00 rows/s., 0.00 B/s.) Received exception from server (version 19.14.1):
Code: 395. DB::Exception: Received from localhost:9000. DB::Exception: Too many.
```

## identité {#identity}

Renvoie la même valeur qui a été utilisée comme argument. Utilisé pour le débogage et les tests, permet d'annuler l'utilisation de l'index et d'obtenir les performances de requête d'une analyse complète. Lorsque la requête est analysée pour une utilisation possible de l'index, l'analyseur ne regarde pas à l'intérieur `identity` fonction.

**Syntaxe**

``` sql
identity(x)
```

**Exemple**

Requête:

``` sql
SELECT identity(42)
```

Résultat:

``` text
┌─identity(42)─┐
│           42 │
└──────────────┘
```

## randomPrintableASCII {#randomascii}

Génère une chaîne avec un ensemble aléatoire de [ASCII](https://en.wikipedia.org/wiki/ASCII#Printable_characters) caractères imprimables.

**Syntaxe**

``` sql
randomPrintableASCII(length)
```

**Paramètre**

-   `length` — Resulting string length. Positive integer.

        If you pass `length < 0`, behavior of the function is undefined.

**Valeur renvoyée**

-   Chaîne avec un ensemble aléatoire de [ASCII](https://en.wikipedia.org/wiki/ASCII#Printable_characters) caractères imprimables.

Type: [Chaîne](../../sql-reference/data-types/string.md)

**Exemple**

``` sql
SELECT number, randomPrintableASCII(30) as str, length(str) FROM system.numbers LIMIT 3
```

``` text
┌─number─┬─str────────────────────────────┬─length(randomPrintableASCII(30))─┐
│      0 │ SuiCOSTvC0csfABSw=UcSzp2.`rv8x │                               30 │
│      1 │ 1Ag NlJ &RCN:*>HVPG;PE-nO"SUFD │                               30 │
│      2 │ /"+<"wUTh:=LjJ Vm!c&hI*m#XTfzz │                               30 │
└────────┴────────────────────────────────┴──────────────────────────────────┘
```

[Article Original](https://clickhouse.tech/docs/en/query_language/functions/other_functions/) <!--hide-->
