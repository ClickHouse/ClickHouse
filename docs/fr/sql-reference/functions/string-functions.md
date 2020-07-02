---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 40
toc_title: "Travailler avec des cha\xEEnes"
---

# Fonctions pour travailler avec des chaînes {#functions-for-working-with-strings}

## vide {#empty}

Renvoie 1 pour une chaîne vide ou 0 pour une chaîne non vide.
Le type de résultat est UInt8.
Une chaîne est considérée comme non vide si elle contient au moins un octet, même s'il s'agit d'un espace ou d'un octet nul.
La fonction fonctionne également pour les tableaux.

## notEmpty {#notempty}

Renvoie 0 pour une chaîne vide ou 1 pour une chaîne non vide.
Le type de résultat est UInt8.
La fonction fonctionne également pour les tableaux.

## longueur {#length}

Renvoie la longueur d'une chaîne en octets (pas en caractères, et pas en points de code).
Le type de résultat est UInt64.
La fonction fonctionne également pour les tableaux.

## lengthUTF8 {#lengthutf8}

Renvoie la longueur d'une chaîne en points de code Unicode (pas en caractères), en supposant que la chaîne contient un ensemble d'octets qui composent le texte codé en UTF-8. Si cette hypothèse n'est pas remplie, elle renvoie un résultat (elle ne lance pas d'exception).
Le type de résultat est UInt64.

## char\_length, CHAR\_LENGTH {#char-length}

Renvoie la longueur d'une chaîne en points de code Unicode (pas en caractères), en supposant que la chaîne contient un ensemble d'octets qui composent le texte codé en UTF-8. Si cette hypothèse n'est pas remplie, elle renvoie un résultat (elle ne lance pas d'exception).
Le type de résultat est UInt64.

## character\_length, CHARACTER\_LENGTH {#character-length}

Renvoie la longueur d'une chaîne en points de code Unicode (pas en caractères), en supposant que la chaîne contient un ensemble d'octets qui composent le texte codé en UTF-8. Si cette hypothèse n'est pas remplie, elle renvoie un résultat (elle ne lance pas d'exception).
Le type de résultat est UInt64.

## plus bas, lcase {#lower}

Convertit les symboles latins ASCII dans une chaîne en minuscules.

## supérieur, ucase {#upper}

Convertit les symboles latins ASCII dans une chaîne en majuscules.

## lowerUTF8 {#lowerutf8}

Convertit une chaîne en minuscules, en supposant que la chaîne de caractères contient un ensemble d'octets qui composent un texte UTF-8.
Il ne détecte pas la langue. Donc, pour le turc, le résultat pourrait ne pas être exactement correct.
Si la longueur de la séquence d'octets UTF-8 est différente pour les majuscules et les minuscules d'un point de code, le résultat peut être incorrect pour ce point de code.
Si la chaîne contient un ensemble d'octets qui N'est pas UTF-8, le comportement n'est pas défini.

## upperUTF8 {#upperutf8}

Convertit une chaîne en majuscules, en supposant que la chaîne de caractères contient un ensemble d'octets qui composent un texte UTF-8.
Il ne détecte pas la langue. Donc, pour le turc, le résultat pourrait ne pas être exactement correct.
Si la longueur de la séquence d'octets UTF-8 est différente pour les majuscules et les minuscules d'un point de code, le résultat peut être incorrect pour ce point de code.
Si la chaîne contient un ensemble d'octets qui N'est pas UTF-8, le comportement n'est pas défini.

## isValidUTF8 {#isvalidutf8}

Renvoie 1, si l'ensemble d'octets est codé en UTF-8 valide, sinon 0.

## toValidUTF8 {#tovalidutf8}

Remplace les caractères UTF-8 non valides par `�` (U+FFFD) caractère. Tous les caractères non valides s'exécutant dans une rangée sont réduits en un seul caractère de remplacement.

``` sql
toValidUTF8( input_string )
```

Paramètre:

-   input\_string — Any set of bytes represented as the [Chaîne](../../sql-reference/data-types/string.md) type de données objet.

Valeur renvoyée: chaîne UTF-8 valide.

**Exemple**

``` sql
SELECT toValidUTF8('\x61\xF0\x80\x80\x80b')
```

``` text
┌─toValidUTF8('a����b')─┐
│ a�b                   │
└───────────────────────┘
```

## répéter {#repeat}

Répète une corde autant de fois que spécifié et concatène les valeurs répliquées comme une seule chaîne.

**Syntaxe**

``` sql
repeat(s, n)
```

**Paramètre**

-   `s` — The string to repeat. [Chaîne](../../sql-reference/data-types/string.md).
-   `n` — The number of times to repeat the string. [UInt](../../sql-reference/data-types/int-uint.md).

**Valeur renvoyée**

La chaîne unique, qui contient la chaîne `s` répéter `n` temps. Si `n` \< 1, la fonction renvoie une chaîne vide.

Type: `String`.

**Exemple**

Requête:

``` sql
SELECT repeat('abc', 10)
```

Résultat:

``` text
┌─repeat('abc', 10)──────────────┐
│ abcabcabcabcabcabcabcabcabcabc │
└────────────────────────────────┘
```

## inverser {#reverse}

Inverse la chaîne (comme une séquence d'octets).

## reverseUTF8 {#reverseutf8}

Inverse une séquence de points de code Unicode, en supposant que la chaîne contient un ensemble d'octets représentant un texte UTF-8. Sinon, il fait autre chose (il ne lance pas d'exception).

## format(pattern, s0, s1, …) {#format}

Formatage du motif constant avec la chaîne listée dans les arguments. `pattern` est un modèle de format Python simplifié. Chaîne de Format contient “replacement fields” entouré par des accolades `{}`. Tout ce qui n'est pas contenu dans les accolades est considéré comme du texte littéral, qui est copié inchangé dans la sortie. Si vous devez inclure un caractère d'Accolade dans le texte littéral, il peut être échappé en doublant: `{{ '{{' }}` et `{{ '}}' }}`. Les noms de champs peuvent être des nombres (à partir de zéro) ou vides (ils sont alors traités comme des nombres de conséquence).

``` sql
SELECT format('{1} {0} {1}', 'World', 'Hello')
```

``` text
┌─format('{1} {0} {1}', 'World', 'Hello')─┐
│ Hello World Hello                       │
└─────────────────────────────────────────┘
```

``` sql
SELECT format('{} {}', 'Hello', 'World')
```

``` text
┌─format('{} {}', 'Hello', 'World')─┐
│ Hello World                       │
└───────────────────────────────────┘
```

## concat {#concat}

Concatène les chaînes répertoriées dans les arguments, sans séparateur.

**Syntaxe**

``` sql
concat(s1, s2, ...)
```

**Paramètre**

Valeurs de type String ou FixedString.

**Valeurs renvoyées**

Renvoie la chaîne qui résulte de la concaténation des arguments.

Si l'une des valeurs d'argument est `NULL`, `concat` retourner `NULL`.

**Exemple**

Requête:

``` sql
SELECT concat('Hello, ', 'World!')
```

Résultat:

``` text
┌─concat('Hello, ', 'World!')─┐
│ Hello, World!               │
└─────────────────────────────┘
```

## concatAssumeInjective {#concatassumeinjective}

Même que [concat](#concat) la différence est que vous devez vous assurer que `concat(s1, s2, ...) → sn` est injectif, il sera utilisé pour l'optimisation du groupe par.

La fonction est nommée “injective” si elle renvoie toujours un résultat différent pour différentes valeurs d'arguments. En d'autres termes: des arguments différents ne donnent jamais un résultat identique.

**Syntaxe**

``` sql
concatAssumeInjective(s1, s2, ...)
```

**Paramètre**

Valeurs de type String ou FixedString.

**Valeurs renvoyées**

Renvoie la chaîne qui résulte de la concaténation des arguments.

Si l'une des valeurs d'argument est `NULL`, `concatAssumeInjective` retourner `NULL`.

**Exemple**

Table d'entrée:

``` sql
CREATE TABLE key_val(`key1` String, `key2` String, `value` UInt32) ENGINE = TinyLog;
INSERT INTO key_val VALUES ('Hello, ','World',1), ('Hello, ','World',2), ('Hello, ','World!',3), ('Hello',', World!',2);
SELECT * from key_val;
```

``` text
┌─key1────┬─key2─────┬─value─┐
│ Hello,  │ World    │     1 │
│ Hello,  │ World    │     2 │
│ Hello,  │ World!   │     3 │
│ Hello   │ , World! │     2 │
└─────────┴──────────┴───────┘
```

Requête:

``` sql
SELECT concat(key1, key2), sum(value) FROM key_val GROUP BY concatAssumeInjective(key1, key2)
```

Résultat:

``` text
┌─concat(key1, key2)─┬─sum(value)─┐
│ Hello, World!      │          3 │
│ Hello, World!      │          2 │
│ Hello, World       │          3 │
└────────────────────┴────────────┘
```

## substring(s, offset, longueur), mid(s, offset, longueur), substr(s, offset, longueur) {#substring}

Renvoie une sous-chaîne commençant par l'octet du ‘offset’ index ‘length’ octets de long. L'indexation des caractères commence à partir d'un (comme dans SQL standard). Le ‘offset’ et ‘length’ les arguments doivent être des constantes.

## substringUTF8(s, offset, longueur) {#substringutf8}

Le même que ‘substring’, mais pour les points de code Unicode. Fonctionne sous l'hypothèse que la chaîne contient un ensemble d'octets représentant un texte codé en UTF-8. Si cette hypothèse n'est pas remplie, elle renvoie un résultat (elle ne lance pas d'exception).

## appendTrailingCharIfAbsent (s, c) {#appendtrailingcharifabsent}

Si l' ‘s’ la chaîne n'est pas vide et ne contient pas ‘c’ personnage à la fin, il ajoute le ‘c’ personnage à la fin.

## convertCharset(s, à partir de, à) {#convertcharset}

Retourne une chaîne de caractères ‘s’ qui a été converti à partir de l'encodage dans ‘from’ pour l'encodage dans ‘to’.

## base64Encode(s) {#base64encode}

Encodage ‘s’ chaîne dans base64

## base64Decode(s) {#base64decode}

Décoder la chaîne codée en base64 ‘s’ dans la chaîne d'origine. En cas d'échec, une exception est levée.

## tryBase64Decode(s) {#trybase64decode}

Semblable à base64Decode, mais en cas d'erreur, une chaîne vide serait renvoyé.

## endsWith (s, suffixe) {#endswith}

Renvoie s'il faut se terminer par le suffixe spécifié. Retourne 1 si la chaîne se termine par le suffixe spécifié, sinon elle renvoie 0.

## startsWith (STR, préfixe) {#startswith}

Retourne 1 si la chaîne commence par le préfixe spécifié, sinon elle renvoie 0.

``` sql
SELECT startsWith('Spider-Man', 'Spi');
```

**Valeurs renvoyées**

-   1, si la chaîne commence par le préfixe spécifié.
-   0, si la chaîne ne commence pas par le préfixe spécifié.

**Exemple**

Requête:

``` sql
SELECT startsWith('Hello, world!', 'He');
```

Résultat:

``` text
┌─startsWith('Hello, world!', 'He')─┐
│                                 1 │
└───────────────────────────────────┘
```

## coupe {#trim}

Supprime tous les caractères spécifiés du début ou de la fin d'une chaîne.
Par défaut supprime toutes les occurrences consécutives d'espaces communs (caractère ASCII 32) des deux extrémités d'une chaîne.

**Syntaxe**

``` sql
trim([[LEADING|TRAILING|BOTH] trim_character FROM] input_string)
```

**Paramètre**

-   `trim_character` — specified characters for trim. [Chaîne](../../sql-reference/data-types/string.md).
-   `input_string` — string for trim. [Chaîne](../../sql-reference/data-types/string.md).

**Valeur renvoyée**

Une chaîne sans caractères de début et (ou) de fin spécifiés.

Type: `String`.

**Exemple**

Requête:

``` sql
SELECT trim(BOTH ' ()' FROM '(   Hello, world!   )')
```

Résultat:

``` text
┌─trim(BOTH ' ()' FROM '(   Hello, world!   )')─┐
│ Hello, world!                                 │
└───────────────────────────────────────────────┘
```

## trimLeft {#trimleft}

Supprime toutes les occurrences consécutives d'espaces communs (caractère ASCII 32) depuis le début d'une chaîne. Il ne supprime pas d'autres types de caractères d'espaces (tabulation, espace sans pause, etc.).

**Syntaxe**

``` sql
trimLeft(input_string)
```

Alias: `ltrim(input_string)`.

**Paramètre**

-   `input_string` — string to trim. [Chaîne](../../sql-reference/data-types/string.md).

**Valeur renvoyée**

Une chaîne sans ouvrir les espaces communs.

Type: `String`.

**Exemple**

Requête:

``` sql
SELECT trimLeft('     Hello, world!     ')
```

Résultat:

``` text
┌─trimLeft('     Hello, world!     ')─┐
│ Hello, world!                       │
└─────────────────────────────────────┘
```

## trimRight {#trimright}

Supprime toutes les occurrences consécutives d'espaces communs (caractère ASCII 32) de la fin d'une chaîne. Il ne supprime pas d'autres types de caractères d'espaces (tabulation, espace sans pause, etc.).

**Syntaxe**

``` sql
trimRight(input_string)
```

Alias: `rtrim(input_string)`.

**Paramètre**

-   `input_string` — string to trim. [Chaîne](../../sql-reference/data-types/string.md).

**Valeur renvoyée**

Une chaîne sans espaces communs de fin.

Type: `String`.

**Exemple**

Requête:

``` sql
SELECT trimRight('     Hello, world!     ')
```

Résultat:

``` text
┌─trimRight('     Hello, world!     ')─┐
│      Hello, world!                   │
└──────────────────────────────────────┘
```

## trimBoth {#trimboth}

Supprime toutes les occurrences consécutives d'espaces communs (caractère ASCII 32) des deux extrémités d'une chaîne. Il ne supprime pas d'autres types de caractères d'espaces (tabulation, espace sans pause, etc.).

**Syntaxe**

``` sql
trimBoth(input_string)
```

Alias: `trim(input_string)`.

**Paramètre**

-   `input_string` — string to trim. [Chaîne](../../sql-reference/data-types/string.md).

**Valeur renvoyée**

Une chaîne sans espaces communs de début et de fin.

Type: `String`.

**Exemple**

Requête:

``` sql
SELECT trimBoth('     Hello, world!     ')
```

Résultat:

``` text
┌─trimBoth('     Hello, world!     ')─┐
│ Hello, world!                       │
└─────────────────────────────────────┘
```

## CRC32 (s) {#crc32}

Renvoie la somme de contrôle CRC32 d'une chaîne, en utilisant le polynôme CRC-32-IEEE 802.3 et la valeur initiale `0xffffffff` (zlib mise en œuvre).

Le type de résultat est UInt32.

## CRC32IEEE (s) {#crc32ieee}

Renvoie la somme de contrôle CRC32 d'une chaîne, en utilisant le polynôme CRC-32-IEEE 802.3.

Le type de résultat est UInt32.

## CRC64 (s) {#crc64}

Renvoie la somme de contrôle CRC64 d'une chaîne, en utilisant le polynôme CRC-64-ECMA.

Le type de résultat est UInt64.

[Article Original](https://clickhouse.tech/docs/en/query_language/functions/string_functions/) <!--hide-->
