---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 50
toc_title: Hachage
---

# Les Fonctions De Hachage {#hash-functions}

Les fonctions de hachage peuvent être utilisées pour le brassage pseudo-aléatoire déterministe des éléments.

## halfMD5 {#hash-functions-halfmd5}

[Interpréter](../../sql-reference/functions/type-conversion-functions.md#type_conversion_functions-reinterpretAsString) tous les paramètres d'entrée sous forme de chaînes et calcule le [MD5](https://en.wikipedia.org/wiki/MD5) la valeur de hachage pour chacun d'eux. Puis combine les hachages, prend les 8 premiers octets du hachage de la chaîne résultante, et les interprète comme `UInt64` dans l'ordre des octets big-endian.

``` sql
halfMD5(par1, ...)
```

La fonction est relativement lente (5 millions de chaînes courtes par seconde par cœur de processeur).
Envisager l'utilisation de la [sipHash64](#hash_functions-siphash64) la fonction la place.

**Paramètre**

La fonction prend un nombre variable de paramètres d'entrée. Les paramètres peuvent être tout de la [types de données pris en charge](../../sql-reference/data-types/index.md).

**Valeur Renvoyée**

A [UInt64](../../sql-reference/data-types/int-uint.md) valeur de hachage du type de données.

**Exemple**

``` sql
SELECT halfMD5(array('e','x','a'), 'mple', 10, toDateTime('2019-06-15 23:00:00')) AS halfMD5hash, toTypeName(halfMD5hash) AS type
```

``` text
┌────────halfMD5hash─┬─type───┐
│ 186182704141653334 │ UInt64 │
└────────────────────┴────────┘
```

## MD5 {#hash_functions-md5}

Calcule le MD5 à partir d'une chaîne et renvoie L'ensemble d'octets résultant en tant que FixedString(16).
Si vous n'avez pas besoin de MD5 en particulier, mais que vous avez besoin d'un hachage cryptographique 128 bits décent, utilisez le ‘sipHash128’ la fonction la place.
Si vous voulez obtenir le même résultat que la sortie de l'utilitaire md5sum, utilisez lower (hex(MD5 (s))).

## sipHash64 {#hash_functions-siphash64}

Produit un 64 bits [SipHash](https://131002.net/siphash/) la valeur de hachage.

``` sql
sipHash64(par1,...)
```

C'est une fonction de hachage cryptographique. Il fonctionne au moins trois fois plus vite que le [MD5](#hash_functions-md5) fonction.

Fonction [interpréter](../../sql-reference/functions/type-conversion-functions.md#type_conversion_functions-reinterpretAsString) tous les paramètres d'entrée sous forme de chaînes et calcule la valeur de hachage pour chacun d'eux. Puis combine les hachages par l'algorithme suivant:

1.  Après avoir haché tous les paramètres d'entrée, la fonction obtient le tableau de hachages.
2.  La fonction prend le premier et le second éléments et calcule un hachage pour le tableau d'entre eux.
3.  Ensuite, la fonction prend la valeur de hachage, calculée à l'étape précédente, et le troisième élément du tableau de hachage initial, et calcule un hachage pour le tableau d'entre eux.
4.  L'étape précédente est répétée pour tous les éléments restants de la période initiale de hachage tableau.

**Paramètre**

La fonction prend un nombre variable de paramètres d'entrée. Les paramètres peuvent être tout de la [types de données pris en charge](../../sql-reference/data-types/index.md).

**Valeur Renvoyée**

A [UInt64](../../sql-reference/data-types/int-uint.md) valeur de hachage du type de données.

**Exemple**

``` sql
SELECT sipHash64(array('e','x','a'), 'mple', 10, toDateTime('2019-06-15 23:00:00')) AS SipHash, toTypeName(SipHash) AS type
```

``` text
┌──────────────SipHash─┬─type───┐
│ 13726873534472839665 │ UInt64 │
└──────────────────────┴────────┘
```

## sipHash128 {#hash_functions-siphash128}

Calcule SipHash à partir d'une chaîne.
Accepte un argument de type chaîne. Renvoie FixedString (16).
Diffère de sipHash64 en ce que l'état de pliage xor final n'est effectué que jusqu'à 128 bits.

## cityHash64 {#cityhash64}

Produit un 64 bits [CityHash](https://github.com/google/cityhash) la valeur de hachage.

``` sql
cityHash64(par1,...)
```

Ceci est une fonction de hachage non cryptographique rapide. Il utilise L'algorithme CityHash pour les paramètres de chaîne et la fonction de hachage rapide non cryptographique spécifique à l'implémentation pour les paramètres avec d'autres types de données. La fonction utilise le combinateur CityHash pour obtenir les résultats finaux.

**Paramètre**

La fonction prend un nombre variable de paramètres d'entrée. Les paramètres peuvent être tout de la [types de données pris en charge](../../sql-reference/data-types/index.md).

**Valeur Renvoyée**

A [UInt64](../../sql-reference/data-types/int-uint.md) valeur de hachage du type de données.

**Exemple**

Appelez exemple:

``` sql
SELECT cityHash64(array('e','x','a'), 'mple', 10, toDateTime('2019-06-15 23:00:00')) AS CityHash, toTypeName(CityHash) AS type
```

``` text
┌─────────────CityHash─┬─type───┐
│ 12072650598913549138 │ UInt64 │
└──────────────────────┴────────┘
```

L'exemple suivant montre comment calculer la somme de l'ensemble de la table avec précision jusqu'à la ligne de commande:

``` sql
SELECT groupBitXor(cityHash64(*)) FROM table
```

## intHash32 {#inthash32}

Calcule un code de hachage 32 bits à partir de n'importe quel type d'entier.
C'est une fonction de hachage non cryptographique relativement rapide de qualité moyenne pour les nombres.

## intHash64 {#inthash64}

Calcule un code de hachage 64 bits à partir de n'importe quel type d'entier.
Il fonctionne plus vite que intHash32. Qualité moyenne.

## SHA1 {#sha1}

## SHA224 {#sha224}

## SHA256 {#sha256}

Calcule SHA-1, SHA-224 ou SHA-256 à partir d'une chaîne et renvoie l'ensemble d'octets résultant en tant que FixedString(20), FixedString(28) ou FixedString(32).
La fonction fonctionne assez lentement (SHA-1 traite environ 5 millions de chaînes courtes par seconde par cœur de processeur, tandis que SHA-224 et SHA-256 traitent environ 2,2 millions).
Nous vous recommandons d'utiliser cette fonction uniquement dans les cas où vous avez besoin d'une fonction de hachage spécifique et que vous ne pouvez pas la sélectionner.
Même dans ces cas, nous vous recommandons d'appliquer la fonction hors ligne et de pré-calculer les valeurs lors de leur insertion dans la table, au lieu de l'appliquer dans SELECTS.

## URLHash(url \[, N\]) {#urlhashurl-n}

Une fonction de hachage non cryptographique rapide et de qualité décente pour une chaîne obtenue à partir d'une URL en utilisant un type de normalisation.
`URLHash(s)` – Calculates a hash from a string without one of the trailing symbols `/`,`?` ou `#` à la fin, si elle est présente.
`URLHash(s, N)` – Calculates a hash from a string up to the N level in the URL hierarchy, without one of the trailing symbols `/`,`?` ou `#` à la fin, si elle est présente.
Les niveaux sont les mêmes que dans URLHierarchy. Cette fonction est spécifique à Yandex.Metrica.

## farmHash64 {#farmhash64}

Produit un 64 bits [FarmHash](https://github.com/google/farmhash) la valeur de hachage.

``` sql
farmHash64(par1, ...)
```

La fonction utilise le `Hash64` la méthode de tous les [les méthodes disponibles](https://github.com/google/farmhash/blob/master/src/farmhash.h).

**Paramètre**

La fonction prend un nombre variable de paramètres d'entrée. Les paramètres peuvent être tout de la [types de données pris en charge](../../sql-reference/data-types/index.md).

**Valeur Renvoyée**

A [UInt64](../../sql-reference/data-types/int-uint.md) valeur de hachage du type de données.

**Exemple**

``` sql
SELECT farmHash64(array('e','x','a'), 'mple', 10, toDateTime('2019-06-15 23:00:00')) AS FarmHash, toTypeName(FarmHash) AS type
```

``` text
┌─────────────FarmHash─┬─type───┐
│ 17790458267262532859 │ UInt64 │
└──────────────────────┴────────┘
```

## javaHash {#hash_functions-javahash}

Calculer [JavaHash](http://hg.openjdk.java.net/jdk8u/jdk8u/jdk/file/478a4add975b/src/share/classes/java/lang/String.java#l1452) à partir d'une chaîne. Cette fonction de hachage n'est ni rapide ni de bonne qualité. La seule raison de l'utiliser est lorsque cet algorithme est déjà utilisé dans un autre système et que vous devez calculer exactement le même résultat.

**Syntaxe**

``` sql
SELECT javaHash('');
```

**Valeur renvoyée**

A `Int32` valeur de hachage du type de données.

**Exemple**

Requête:

``` sql
SELECT javaHash('Hello, world!');
```

Résultat:

``` text
┌─javaHash('Hello, world!')─┐
│               -1880044555 │
└───────────────────────────┘
```

## javaHashUTF16LE {#javahashutf16le}

Calculer [JavaHash](http://hg.openjdk.java.net/jdk8u/jdk8u/jdk/file/478a4add975b/src/share/classes/java/lang/String.java#l1452) à partir d'une chaîne, en supposant qu'elle contient des octets représentant une chaîne en encodage UTF-16LE.

**Syntaxe**

``` sql
javaHashUTF16LE(stringUtf16le)
```

**Paramètre**

-   `stringUtf16le` — a string in UTF-16LE encoding.

**Valeur renvoyée**

A `Int32` valeur de hachage du type de données.

**Exemple**

Requête correcte avec une chaîne codée UTF-16LE.

Requête:

``` sql
SELECT javaHashUTF16LE(convertCharset('test', 'utf-8', 'utf-16le'))
```

Résultat:

``` text
┌─javaHashUTF16LE(convertCharset('test', 'utf-8', 'utf-16le'))─┐
│                                                      3556498 │
└──────────────────────────────────────────────────────────────┘
```

## hiveHash {#hash-functions-hivehash}

Calculer `HiveHash` à partir d'une chaîne.

``` sql
SELECT hiveHash('');
```

C'est juste [JavaHash](#hash_functions-javahash) avec le bit de signe mis à zéro. Cette fonction est utilisée dans [Apache Hive](https://en.wikipedia.org/wiki/Apache_Hive) pour les versions antérieures à la version 3.0. Cette fonction de hachage n'est ni rapide ni de bonne qualité. La seule raison de l'utiliser est lorsque cet algorithme est déjà utilisé dans un autre système et que vous devez calculer exactement le même résultat.

**Valeur renvoyée**

A `Int32` valeur de hachage du type de données.

Type: `hiveHash`.

**Exemple**

Requête:

``` sql
SELECT hiveHash('Hello, world!');
```

Résultat:

``` text
┌─hiveHash('Hello, world!')─┐
│                 267439093 │
└───────────────────────────┘
```

## metroHash64 {#metrohash64}

Produit un 64 bits [MetroHash](http://www.jandrewrogers.com/2015/05/27/metrohash/) la valeur de hachage.

``` sql
metroHash64(par1, ...)
```

**Paramètre**

La fonction prend un nombre variable de paramètres d'entrée. Les paramètres peuvent être tout de la [types de données pris en charge](../../sql-reference/data-types/index.md).

**Valeur Renvoyée**

A [UInt64](../../sql-reference/data-types/int-uint.md) valeur de hachage du type de données.

**Exemple**

``` sql
SELECT metroHash64(array('e','x','a'), 'mple', 10, toDateTime('2019-06-15 23:00:00')) AS MetroHash, toTypeName(MetroHash) AS type
```

``` text
┌────────────MetroHash─┬─type───┐
│ 14235658766382344533 │ UInt64 │
└──────────────────────┴────────┘
```

## jumpConsistentHash {#jumpconsistenthash}

Calcule JumpConsistentHash forme un UInt64.
Accepte deux arguments: une clé de type UInt64 et le nombre de compartiments. Renvoie Int32.
Pour plus d'informations, voir le lien: [JumpConsistentHash](https://arxiv.org/pdf/1406.2294.pdf)

## murmurHash2\_32, murmurHash2\_64 {#murmurhash2-32-murmurhash2-64}

Produit un [MurmurHash2](https://github.com/aappleby/smhasher) la valeur de hachage.

``` sql
murmurHash2_32(par1, ...)
murmurHash2_64(par1, ...)
```

**Paramètre**

Les deux fonctions prennent un nombre variable de paramètres d'entrée. Les paramètres peuvent être tout de la [types de données pris en charge](../../sql-reference/data-types/index.md).

**Valeur Renvoyée**

-   Le `murmurHash2_32` fonction renvoie la valeur de hachage ayant le [UInt32](../../sql-reference/data-types/int-uint.md) type de données.
-   Le `murmurHash2_64` fonction renvoie la valeur de hachage ayant le [UInt64](../../sql-reference/data-types/int-uint.md) type de données.

**Exemple**

``` sql
SELECT murmurHash2_64(array('e','x','a'), 'mple', 10, toDateTime('2019-06-15 23:00:00')) AS MurmurHash2, toTypeName(MurmurHash2) AS type
```

``` text
┌──────────MurmurHash2─┬─type───┐
│ 11832096901709403633 │ UInt64 │
└──────────────────────┴────────┘
```

## gccMurmurHash {#gccmurmurhash}

Calcule un 64 bits [MurmurHash2](https://github.com/aappleby/smhasher) valeur de hachage utilisant la même graine de hachage que [gcc](https://github.com/gcc-mirror/gcc/blob/41d6b10e96a1de98e90a7c0378437c3255814b16/libstdc%2B%2B-v3/include/bits/functional_hash.h#L191). Il est portable entre Clang et GCC construit.

**Syntaxe**

``` sql
gccMurmurHash(par1, ...);
```

**Paramètre**

-   `par1, ...` — A variable number of parameters that can be any of the [types de données pris en charge](../../sql-reference/data-types/index.md#data_types).

**Valeur renvoyée**

-   Valeur de hachage calculée.

Type: [UInt64](../../sql-reference/data-types/int-uint.md).

**Exemple**

Requête:

``` sql
SELECT
    gccMurmurHash(1, 2, 3) AS res1,
    gccMurmurHash(('a', [1, 2, 3], 4, (4, ['foo', 'bar'], 1, (1, 2)))) AS res2
```

Résultat:

``` text
┌─────────────────res1─┬────────────────res2─┐
│ 12384823029245979431 │ 1188926775431157506 │
└──────────────────────┴─────────────────────┘
```

## murmurHash3\_32, murmurHash3\_64 {#murmurhash3-32-murmurhash3-64}

Produit un [MurmurHash3](https://github.com/aappleby/smhasher) la valeur de hachage.

``` sql
murmurHash3_32(par1, ...)
murmurHash3_64(par1, ...)
```

**Paramètre**

Les deux fonctions prennent un nombre variable de paramètres d'entrée. Les paramètres peuvent être tout de la [types de données pris en charge](../../sql-reference/data-types/index.md).

**Valeur Renvoyée**

-   Le `murmurHash3_32` la fonction retourne un [UInt32](../../sql-reference/data-types/int-uint.md) valeur de hachage du type de données.
-   Le `murmurHash3_64` la fonction retourne un [UInt64](../../sql-reference/data-types/int-uint.md) valeur de hachage du type de données.

**Exemple**

``` sql
SELECT murmurHash3_32(array('e','x','a'), 'mple', 10, toDateTime('2019-06-15 23:00:00')) AS MurmurHash3, toTypeName(MurmurHash3) AS type
```

``` text
┌─MurmurHash3─┬─type───┐
│     2152717 │ UInt32 │
└─────────────┴────────┘
```

## murmurHash3\_128 {#murmurhash3-128}

Produit de 128 bits [MurmurHash3](https://github.com/aappleby/smhasher) la valeur de hachage.

``` sql
murmurHash3_128( expr )
```

**Paramètre**

-   `expr` — [Expression](../syntax.md#syntax-expressions) de retour d'un [Chaîne](../../sql-reference/data-types/string.md)-le type de la valeur.

**Valeur Renvoyée**

A [FixedString (16)](../../sql-reference/data-types/fixedstring.md) valeur de hachage du type de données.

**Exemple**

``` sql
SELECT murmurHash3_128('example_string') AS MurmurHash3, toTypeName(MurmurHash3) AS type
```

``` text
┌─MurmurHash3──────┬─type────────────┐
│ 6�1�4"S5KT�~~q │ FixedString(16) │
└──────────────────┴─────────────────┘
```

## xxHash32, xxHash64 {#hash-functions-xxhash32}

Calculer `xxHash` à partir d'une chaîne. Il est proposé en deux saveurs, 32 et 64 bits.

``` sql
SELECT xxHash32('');

OR

SELECT xxHash64('');
```

**Valeur renvoyée**

A `Uint32` ou `Uint64` valeur de hachage du type de données.

Type: `xxHash`.

**Exemple**

Requête:

``` sql
SELECT xxHash32('Hello, world!');
```

Résultat:

``` text
┌─xxHash32('Hello, world!')─┐
│                 834093149 │
└───────────────────────────┘
```

**Voir Aussi**

-   [xxHash](http://cyan4973.github.io/xxHash/).

[Article Original](https://clickhouse.tech/docs/en/query_language/functions/hash_functions/) <!--hide-->
