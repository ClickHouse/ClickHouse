---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 50
toc_title: Enum
---

# Enum {#enum}

Type énuméré composé de valeurs nommées.

Les valeurs nommées doivent être déclarées comme `'string' = integer` pair. ClickHouse ne stocke que des nombres, mais prend en charge les opérations avec les valeurs à travers leurs noms.

Supports ClickHouse:

-   8-bit `Enum`. Il peut contenir jusqu'à 256 valeurs énumérées dans le `[-128, 127]` gamme.
-   16 bits `Enum`. Il peut contenir jusqu'à 65 536 valeurs énumérées dans le `[-32768, 32767]` gamme.

Clickhouse choisit automatiquement le type de `Enum` lorsque les données sont insérées. Vous pouvez également utiliser `Enum8` ou `Enum16` types pour être sûr de la taille de stockage.

## Exemples D'Utilisation {#usage-examples}

Ici, nous créons une table avec une `Enum8('hello' = 1, 'world' = 2)` type de colonne:

``` sql
CREATE TABLE t_enum
(
    x Enum('hello' = 1, 'world' = 2)
)
ENGINE = TinyLog
```

Colonne `x` ne peut stocker que les valeurs répertoriées dans la définition de type: `'hello'` ou `'world'`. Si vous essayez d'enregistrer une autre valeur, ClickHouse déclenchera une exception. Taille 8 bits pour cela `Enum` est choisi automatiquement.

``` sql
INSERT INTO t_enum VALUES ('hello'), ('world'), ('hello')
```

``` text
Ok.
```

``` sql
INSERT INTO t_enum values('a')
```

``` text
Exception on client:
Code: 49. DB::Exception: Unknown element 'a' for type Enum('hello' = 1, 'world' = 2)
```

Lorsque vous interrogez des données de la table, ClickHouse affiche les valeurs de chaîne de `Enum`.

``` sql
SELECT * FROM t_enum
```

``` text
┌─x─────┐
│ hello │
│ world │
│ hello │
└───────┘
```

Si vous avez besoin de voir les équivalents numériques des lignes, vous devez `Enum` valeur en type entier.

``` sql
SELECT CAST(x, 'Int8') FROM t_enum
```

``` text
┌─CAST(x, 'Int8')─┐
│               1 │
│               2 │
│               1 │
└─────────────────┘
```

Pour créer une valeur d'Enum dans une requête, vous devez également utiliser `CAST`.

``` sql
SELECT toTypeName(CAST('a', 'Enum(\'a\' = 1, \'b\' = 2)'))
```

``` text
┌─toTypeName(CAST('a', 'Enum(\'a\' = 1, \'b\' = 2)'))─┐
│ Enum8('a' = 1, 'b' = 2)                             │
└─────────────────────────────────────────────────────┘
```

## Règles générales et utilisation {#general-rules-and-usage}

Chacune des valeurs se voit attribuer un nombre dans la plage `-128 ... 127` pour `Enum8` ou dans la gamme `-32768 ... 32767` pour `Enum16`. Toutes les chaînes et les nombres doivent être différents. Une chaîne vide est autorisé. Si ce type est spécifié (dans une définition de table), les nombres peuvent être dans un ordre arbitraire. Toutefois, l'ordre n'a pas d'importance.

Ni la chaîne ni la valeur numérique dans un `Enum` peut être [NULL](../../sql-reference/syntax.md).

Un `Enum` peut être contenue dans [Nullable](nullable.md) type. Donc, si vous créez une table en utilisant la requête

``` sql
CREATE TABLE t_enum_nullable
(
    x Nullable( Enum8('hello' = 1, 'world' = 2) )
)
ENGINE = TinyLog
```

il peut stocker non seulement des `'hello'` et `'world'`, mais `NULL`, ainsi.

``` sql
INSERT INTO t_enum_nullable Values('hello'),('world'),(NULL)
```

Dans la mémoire RAM, un `Enum` la colonne est stockée dans la même manière que `Int8` ou `Int16` des valeurs numériques correspondantes.

Lors de la lecture sous forme de texte, ClickHouse analyse la valeur sous forme de chaîne et recherche la chaîne correspondante à partir de l'ensemble des valeurs Enum. Si elle n'est pas trouvée, une exception est levée. Lors de la lecture au format texte, la chaîne est lue et la valeur numérique correspondante est recherchée. Une exception sera levée si il n'est pas trouvé.
Lors de l'écriture sous forme de texte, il écrit la valeur correspondante de la chaîne. Si les données de colonne contiennent des déchets (nombres qui ne proviennent pas de l'ensemble valide), une exception est levée. Lors de la lecture et de l'écriture sous forme binaire, cela fonctionne de la même manière que pour les types de données Int8 et Int16.
La valeur implicite par défaut est la valeur avec le numéro le plus bas.

Lors `ORDER BY`, `GROUP BY`, `IN`, `DISTINCT` et ainsi de suite, les Énumérations se comportent de la même façon que les nombres correspondants. Par exemple, ORDER BY les trie numériquement. Les opérateurs d'égalité et de comparaison fonctionnent de la même manière sur les énumérations que sur les valeurs numériques sous-jacentes.

Les valeurs Enum ne peuvent pas être comparées aux nombres. Les Enums peuvent être comparés à une chaîne constante. Si la chaîne comparée à n'est pas une valeur valide pour L'énumération, une exception sera levée. L'opérateur est pris en charge avec l'Enum sur le côté gauche, et un ensemble de chaînes sur le côté droit. Les chaînes sont les valeurs de L'énumération correspondante.

Most numeric and string operations are not defined for Enum values, e.g. adding a number to an Enum or concatenating a string to an Enum.
Cependant, L'énumération a un naturel `toString` fonction qui renvoie sa valeur de chaîne.

Les valeurs Enum sont également convertibles en types numériques en utilisant `toT` fonction, où T est un type numérique. Lorsque T correspond au type numérique sous-jacent de l'énumération, cette conversion est à coût nul.
Le type Enum peut être modifié sans coût en utilisant ALTER, si seulement l'ensemble des valeurs est modifié. Il est possible d'ajouter et de supprimer des membres de L'énumération en utilisant ALTER (la suppression n'est sûre que si la valeur supprimée n'a jamais été utilisée dans la table). À titre de sauvegarde, la modification de la valeur numérique d'un membre Enum précédemment défini lancera une exception.

En utilisant ALTER, il est possible de changer un Enum8 en Enum16 ou vice versa, tout comme changer un Int8 en Int16.

[Article Original](https://clickhouse.tech/docs/en/data_types/enum/) <!--hide-->
