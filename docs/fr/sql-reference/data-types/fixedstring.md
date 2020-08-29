---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 45
toc_title: FixedString (N)
---

# Fixedstring {#fixedstring}

Une chaîne de longueur fixe de `N` octets (ni caractères ni points de code).

Pour déclarer une colonne de `FixedString` tapez, utilisez la syntaxe suivante:

``` sql
<column_name> FixedString(N)
```

Où `N` est un nombre naturel.

Le `FixedString` type est efficace lorsque les données ont la longueur de précisément `N` octet. Dans tous les autres cas, il est susceptible de réduire l'efficacité.

Exemples de valeurs qui peuvent être stockées efficacement dans `FixedString`-tapé colonnes:

-   La représentation binaire des adresses IP (`FixedString(16)` pour IPv6).
-   Language codes (ru\_RU, en\_US … ).
-   Currency codes (USD, RUB … ).
-   Représentation binaire des hachages (`FixedString(16)` pour MD5, `FixedString(32)` pour SHA256).

Pour stocker les valeurs UUID, utilisez [UUID](uuid.md) type de données.

Lors de l'insertion des données, ClickHouse:

-   Complète une chaîne avec des octets null si la chaîne contient moins de `N` octet.
-   Jette le `Too large value for FixedString(N)` exception si la chaîne contient plus de `N` octet.

Lors de la sélection des données, ClickHouse ne supprime pas les octets nuls à la fin de la chaîne. Si vous utilisez le `WHERE` clause, vous devez ajouter des octets null manuellement pour `FixedString` valeur. L'exemple suivant illustre l'utilisation de l' `WHERE` la clause de `FixedString`.

Considérons le tableau suivant avec le seul `FixedString(2)` colonne:

``` text
┌─name──┐
│ b     │
└───────┘
```

Requête `SELECT * FROM FixedStringTable WHERE a = 'b'` ne renvoie aucune donnée en conséquence. Nous devrions compléter le modèle de filtre avec des octets nuls.

``` sql
SELECT * FROM FixedStringTable
WHERE a = 'b\0'
```

``` text
┌─a─┐
│ b │
└───┘
```

Ce comportement diffère de MySQL pour le `CHAR` type (où les chaînes sont remplies d'espaces et les espaces sont supprimés pour la sortie).

À noter que la longueur de la `FixedString(N)` la valeur est constante. Le [longueur](../../sql-reference/functions/array-functions.md#array_functions-length) la fonction renvoie `N` même si l' `FixedString(N)` la valeur est remplie uniquement avec des octets [vide](../../sql-reference/functions/string-functions.md#empty) la fonction renvoie `1` dans ce cas.

[Article Original](https://clickhouse.tech/docs/en/data_types/fixedstring/) <!--hide-->
