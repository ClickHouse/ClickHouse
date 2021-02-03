---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 52
toc_title: Encodage
---

# L'Encodage Des Fonctions {#encoding-functions}

## char {#char}

Retourne la chaîne avec la longueur que le nombre d'arguments passés et chaque octet a la valeur de l'argument correspondant. Accepte plusieurs arguments de types numériques. Si la valeur de l'argument est hors de portée du type de données UInt8, elle est convertie en UInt8 avec arrondi et débordement possibles.

**Syntaxe**

``` sql
char(number_1, [number_2, ..., number_n]);
```

**Paramètre**

-   `number_1, number_2, ..., number_n` — Numerical arguments interpreted as integers. Types: [Int](../../sql-reference/data-types/int-uint.md), [Flottant](../../sql-reference/data-types/float.md).

**Valeur renvoyée**

-   une chaîne d'octets.

Type: `String`.

**Exemple**

Requête:

``` sql
SELECT char(104.1, 101, 108.9, 108.9, 111) AS hello
```

Résultat:

``` text
┌─hello─┐
│ hello │
└───────┘
```

Vous pouvez construire une chaîne de codage arbitraire en passant les octets correspondants. Voici un exemple pour UTF-8:

Requête:

``` sql
SELECT char(0xD0, 0xBF, 0xD1, 0x80, 0xD0, 0xB8, 0xD0, 0xB2, 0xD0, 0xB5, 0xD1, 0x82) AS hello;
```

Résultat:

``` text
┌─hello──┐
│ привет │
└────────┘
```

Requête:

``` sql
SELECT char(0xE4, 0xBD, 0xA0, 0xE5, 0xA5, 0xBD) AS hello;
```

Résultat:

``` text
┌─hello─┐
│ 你好  │
└───────┘
```

## Hex {#hex}

Renvoie une chaîne contenant la représentation hexadécimale de l'argument.

**Syntaxe**

``` sql
hex(arg)
```

La fonction utilise des lettres majuscules `A-F` et ne pas utiliser de préfixes (comme `0x`) ou suffixes (comme `h`).

Pour les arguments entiers, il imprime des chiffres hexadécimaux (“nibbles”) du plus significatif au moins significatif (big endian ou “human readable” ordre). Il commence par l'octet non nul le plus significatif (les octets de début zéro sont omis) mais imprime toujours les deux chiffres de chaque octet même si le chiffre de début est nul.

Exemple:

**Exemple**

Requête:

``` sql
SELECT hex(1);
```

Résultat:

``` text
01
```

Les valeurs de type `Date` et `DateTime` sont formatés comme des entiers correspondants (le nombre de jours depuis Epoch pour Date et la valeur de L'horodatage Unix pour DateTime).

Pour `String` et `FixedString`, tous les octets sont simplement codés en deux nombres hexadécimaux. Zéro octets ne sont pas omis.

Les valeurs des types virgule flottante et décimale sont codées comme leur représentation en mémoire. Comme nous soutenons l'architecture little endian, ils sont codés dans little endian. Zéro octets de début / fin ne sont pas omis.

**Paramètre**

-   `arg` — A value to convert to hexadecimal. Types: [Chaîne](../../sql-reference/data-types/string.md), [UInt](../../sql-reference/data-types/int-uint.md), [Flottant](../../sql-reference/data-types/float.md), [Décimal](../../sql-reference/data-types/decimal.md), [Date](../../sql-reference/data-types/date.md) ou [DateTime](../../sql-reference/data-types/datetime.md).

**Valeur renvoyée**

-   Une chaîne avec la représentation hexadécimale de l'argument.

Type: `String`.

**Exemple**

Requête:

``` sql
SELECT hex(toFloat32(number)) as hex_presentation FROM numbers(15, 2);
```

Résultat:

``` text
┌─hex_presentation─┐
│ 00007041         │
│ 00008041         │
└──────────────────┘
```

Requête:

``` sql
SELECT hex(toFloat64(number)) as hex_presentation FROM numbers(15, 2);
```

Résultat:

``` text
┌─hex_presentation─┐
│ 0000000000002E40 │
│ 0000000000003040 │
└──────────────────┘
```

## unhex (str) {#unhexstr}

Accepte une chaîne contenant un nombre quelconque de chiffres hexadécimaux, et renvoie une chaîne contenant le correspondant octets. Prend en charge les lettres majuscules et minuscules A-F. Le nombre de chiffres hexadécimaux ne doit pas être pair. S'il est impair, le dernier chiffre est interprété comme la moitié la moins significative de l'octet 00-0F. Si la chaîne d'argument contient autre chose que des chiffres hexadécimaux, un résultat défini par l'implémentation est renvoyé (une exception n'est pas levée).
Si vous voulez convertir le résultat en un nombre, vous pouvez utiliser le ‘reverse’ et ‘reinterpretAsType’ fonction.

## UUIDStringToNum (str) {#uuidstringtonumstr}

Accepte une chaîne contenant 36 caractères dans le format `123e4567-e89b-12d3-a456-426655440000`, et le renvoie comme un ensemble d'octets dans un FixedString (16).

## UUIDNumToString (str) {#uuidnumtostringstr}

Accepte une valeur FixedString (16). Renvoie une chaîne contenant 36 caractères au format texte.

## bitmaskToList(num) {#bitmasktolistnum}

Accepte un entier. Renvoie une chaîne contenant la liste des puissances de deux qui totalisent le nombre source lorsqu'il est additionné. Ils sont séparés par des virgules sans espaces au format texte, dans l'ordre croissant.

## bitmaskToArray(num) {#bitmasktoarraynum}

Accepte un entier. Renvoie un tableau de nombres UInt64 contenant la liste des puissances de deux qui totalisent le nombre source lorsqu'il est additionné. Les numéros dans le tableau sont dans l'ordre croissant.

[Article Original](https://clickhouse.tech/docs/en/query_language/functions/encoding_functions/) <!--hide-->
