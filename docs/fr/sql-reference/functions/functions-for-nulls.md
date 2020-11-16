---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 63
toc_title: Travailler avec des arguments nullables
---

# Fonctions pour travailler avec des agrégats nullables {#functions-for-working-with-nullable-aggregates}

## isNull {#isnull}

Vérifie si l'argument est [NULL](../../sql-reference/syntax.md#null-literal).

``` sql
isNull(x)
```

**Paramètre**

-   `x` — A value with a non-compound data type.

**Valeur renvoyée**

-   `1` si `x` être `NULL`.
-   `0` si `x` n'est pas `NULL`.

**Exemple**

Table d'entrée

``` text
┌─x─┬────y─┐
│ 1 │ ᴺᵁᴸᴸ │
│ 2 │    3 │
└───┴──────┘
```

Requête

``` sql
SELECT x FROM t_null WHERE isNull(y)
```

``` text
┌─x─┐
│ 1 │
└───┘
```

## isNotNull {#isnotnull}

Vérifie si l'argument est [NULL](../../sql-reference/syntax.md#null-literal).

``` sql
isNotNull(x)
```

**Paramètre:**

-   `x` — A value with a non-compound data type.

**Valeur renvoyée**

-   `0` si `x` être `NULL`.
-   `1` si `x` n'est pas `NULL`.

**Exemple**

Table d'entrée

``` text
┌─x─┬────y─┐
│ 1 │ ᴺᵁᴸᴸ │
│ 2 │    3 │
└───┴──────┘
```

Requête

``` sql
SELECT x FROM t_null WHERE isNotNull(y)
```

``` text
┌─x─┐
│ 2 │
└───┘
```

## fusionner {#coalesce}

Vérifie de gauche à droite si `NULL` les arguments ont été passés et renvoie le premier non-`NULL` argument.

``` sql
coalesce(x,...)
```

**Paramètre:**

-   N'importe quel nombre de paramètres d'un type non composé. Tous les paramètres doivent être compatibles par type de données.

**Valeurs renvoyées**

-   Le premier non-`NULL` argument.
-   `NULL` si tous les arguments sont `NULL`.

**Exemple**

Considérez une liste de contacts qui peuvent spécifier plusieurs façons de contacter un client.

``` text
┌─name─────┬─mail─┬─phone─────┬──icq─┐
│ client 1 │ ᴺᵁᴸᴸ │ 123-45-67 │  123 │
│ client 2 │ ᴺᵁᴸᴸ │ ᴺᵁᴸᴸ      │ ᴺᵁᴸᴸ │
└──────────┴──────┴───────────┴──────┘
```

Le `mail` et `phone` les champs sont de type Chaîne de caractères, mais la `icq` le terrain est `UInt32`, de sorte qu'il doit être converti en `String`.

Obtenir la première méthode de contact pour le client à partir de la liste de contacts:

``` sql
SELECT coalesce(mail, phone, CAST(icq,'Nullable(String)')) FROM aBook
```

``` text
┌─name─────┬─coalesce(mail, phone, CAST(icq, 'Nullable(String)'))─┐
│ client 1 │ 123-45-67                                            │
│ client 2 │ ᴺᵁᴸᴸ                                                 │
└──────────┴──────────────────────────────────────────────────────┘
```

## ifNull {#ifnull}

Renvoie une valeur alternative si l'argument principal est `NULL`.

``` sql
ifNull(x,alt)
```

**Paramètre:**

-   `x` — The value to check for `NULL`.
-   `alt` — The value that the function returns if `x` être `NULL`.

**Valeurs renvoyées**

-   Valeur `x`, si `x` n'est pas `NULL`.
-   Valeur `alt`, si `x` être `NULL`.

**Exemple**

``` sql
SELECT ifNull('a', 'b')
```

``` text
┌─ifNull('a', 'b')─┐
│ a                │
└──────────────────┘
```

``` sql
SELECT ifNull(NULL, 'b')
```

``` text
┌─ifNull(NULL, 'b')─┐
│ b                 │
└───────────────────┘
```

## nullIf {#nullif}

Retourner `NULL` si les arguments sont égaux.

``` sql
nullIf(x, y)
```

**Paramètre:**

`x`, `y` — Values for comparison. They must be compatible types, or ClickHouse will generate an exception.

**Valeurs renvoyées**

-   `NULL` si les arguments sont égaux.
-   Le `x` valeur, si les arguments ne sont pas égaux.

**Exemple**

``` sql
SELECT nullIf(1, 1)
```

``` text
┌─nullIf(1, 1)─┐
│         ᴺᵁᴸᴸ │
└──────────────┘
```

``` sql
SELECT nullIf(1, 2)
```

``` text
┌─nullIf(1, 2)─┐
│            1 │
└──────────────┘
```

## assumeNotNull {#assumenotnull}

Résultats dans une valeur de type [Nullable](../../sql-reference/data-types/nullable.md) pour un non- `Nullable` si la valeur n'est pas `NULL`.

``` sql
assumeNotNull(x)
```

**Paramètre:**

-   `x` — The original value.

**Valeurs renvoyées**

-   La valeur d'origine du non-`Nullable` type, si elle n'est pas `NULL`.
-   La valeur par défaut pour le non-`Nullable` Tapez si la valeur d'origine était `NULL`.

**Exemple**

Envisager l' `t_null` table.

``` sql
SHOW CREATE TABLE t_null
```

``` text
┌─statement─────────────────────────────────────────────────────────────────┐
│ CREATE TABLE default.t_null ( x Int8,  y Nullable(Int8)) ENGINE = TinyLog │
└───────────────────────────────────────────────────────────────────────────┘
```

``` text
┌─x─┬────y─┐
│ 1 │ ᴺᵁᴸᴸ │
│ 2 │    3 │
└───┴──────┘
```

Appliquer le `assumeNotNull` la fonction de la `y` colonne.

``` sql
SELECT assumeNotNull(y) FROM t_null
```

``` text
┌─assumeNotNull(y)─┐
│                0 │
│                3 │
└──────────────────┘
```

``` sql
SELECT toTypeName(assumeNotNull(y)) FROM t_null
```

``` text
┌─toTypeName(assumeNotNull(y))─┐
│ Int8                         │
│ Int8                         │
└──────────────────────────────┘
```

## toNullable {#tonullable}

Convertit le type d'argument en `Nullable`.

``` sql
toNullable(x)
```

**Paramètre:**

-   `x` — The value of any non-compound type.

**Valeur renvoyée**

-   La valeur d'entrée avec un `Nullable` type.

**Exemple**

``` sql
SELECT toTypeName(10)
```

``` text
┌─toTypeName(10)─┐
│ UInt8          │
└────────────────┘
```

``` sql
SELECT toTypeName(toNullable(10))
```

``` text
┌─toTypeName(toNullable(10))─┐
│ Nullable(UInt8)            │
└────────────────────────────┘
```

[Article Original](https://clickhouse.tech/docs/en/query_language/functions/functions_for_nulls/) <!--hide-->
