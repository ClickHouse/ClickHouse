---
description: 'Documentation sur le type de données Enum dans ClickHouse, qui représente
  un ensemble de valeurs constantes nommées'
sidebar_label: 'Enum'
sidebar_position: 20
slug: /sql-reference/data-types/enum
title: 'Enum'
doc_type: 'reference'
---

Type énuméré composé de valeurs nommées.

Les valeurs nommées peuvent être déclarées sous forme de paires `'string' = integer` ou de noms `'string'`. ClickHouse ne stocke que des nombres, mais permet d’effectuer des opérations sur ces valeurs via leurs noms.

ClickHouse prend en charge :

* `Enum` sur 8 bits. Il peut contenir jusqu’à 256 valeurs énumérées dans la plage `[-128, 127]`.
* `Enum` sur 16 bits. Il peut contenir jusqu’à 65536 valeurs énumérées dans la plage `[-32768, 32767]`.

ClickHouse choisit automatiquement le type d’`Enum` lors de l’insertion des données. Vous pouvez également utiliser les types `Enum8` ou `Enum16` pour maîtriser la taille de stockage.

<div id="usage-examples">
  ## Exemples d’utilisation
</div>

Nous créons ici une table avec une colonne de type `Enum8('hello' = 1, 'world' = 2)` :

```sql
CREATE TABLE t_enum
(
    x Enum('hello' = 1, 'world' = 2)
)
ENGINE = TinyLog
```

De même, vous pouvez omettre les numéros. ClickHouse attribuera automatiquement des numéros consécutifs. Par défaut, les numéros sont attribués à partir de 1.

```sql
CREATE TABLE t_enum
(
    x Enum('hello', 'world')
)
ENGINE = TinyLog
```

Vous pouvez également spécifier un numéro de départ autorisé pour le nom.

```sql
CREATE TABLE t_enum
(
    x Enum('hello' = 1, 'world')
)
ENGINE = TinyLog
```

```sql
CREATE TABLE t_enum
(
    x Enum8('hello' = -129, 'world')
)
ENGINE = TinyLog
```

```text
Exception on server:
Code: 69. DB::Exception: Value -129 for element 'hello' exceeds range of Enum8.
```

La colonne `x` ne peut stocker que les valeurs listées dans la définition du type : `'hello'` ou `'world'`. Si vous essayez d&#39;enregistrer une autre valeur, ClickHouse lèvera une exception. La taille de 8 bits pour cet `Enum` est choisie automatiquement.

```sql
INSERT INTO t_enum VALUES ('hello'), ('world'), ('hello')
```

```text
Ok.
```

```sql
INSERT INTO t_enum VALUES('a')
```

```text
Exception on client:
Code: 49. DB::Exception: Unknown element 'a' for type Enum('hello' = 1, 'world' = 2)
```

Lorsque vous interrogez des données dans la table, ClickHouse renvoie les valeurs textuelles de `Enum`.

```sql
SELECT * FROM t_enum
```

```text
┌─x─────┐
│ hello │
│ world │
│ hello │
└───────┘
```

Si vous devez voir les équivalents numériques des lignes, vous devez convertir la valeur `Enum` en entier.

```sql
SELECT CAST(x, 'Int8') FROM t_enum
```

```text
┌─CAST(x, 'Int8')─┐
│               1 │
│               2 │
│               1 │
└─────────────────┘
```

Pour créer une valeur Enum dans une requête, vous devez aussi utiliser `CAST`.

```sql
SELECT toTypeName(CAST('a', 'Enum(\'a\' = 1, \'b\' = 2)'))
```

```text
┌─toTypeName(CAST('a', 'Enum(\'a\' = 1, \'b\' = 2)'))─┐
│ Enum8('a' = 1, 'b' = 2)                             │
└─────────────────────────────────────────────────────┘
```

<div id="general-rules-and-usage">
  ## Règles générales et utilisation
</div>

À chacune des valeurs est attribué un nombre compris dans l’intervalle `-128 ... 127` pour `Enum8` ou dans l’intervalle `-32768 ... 32767` pour `Enum16`. Toutes les chaînes et tous les nombres doivent être distincts. Une chaîne vide est autorisée. Si ce type est spécifié (dans une définition de table), les nombres peuvent être dans un ordre arbitraire. Toutefois, cet ordre n’a aucune importance.

Ni la chaîne ni la valeur numérique d’un `Enum` ne peuvent être [NULL](../../sql-reference/syntax.md).

Un `Enum` peut être inclus dans le type [Nullable](../../sql-reference/data-types/nullable.md). Ainsi, si vous créez une table à l’aide de la requête

```sql
CREATE TABLE t_enum_nullable
(
    x Nullable( Enum8('hello' = 1, 'world' = 2) )
)
ENGINE = TinyLog
```

il peut stocker non seulement `'hello'` et `'world'`, mais aussi `NULL`.

```sql
INSERT INTO t_enum_nullable VALUES('hello'),('world'),(NULL)
```

En RAM, une colonne `Enum` est stockée de la même manière qu’un `Int8` ou un `Int16` des valeurs numériques correspondantes.

Lors de la lecture sous forme textuelle, ClickHouse analyse la valeur comme une chaîne et recherche la chaîne correspondante parmi l’ensemble des valeurs de l’Enum. Si elle n’est pas trouvée, une exception est levée. Lors de la lecture au format texte, la chaîne est lue et la valeur numérique correspondante est recherchée. Une exception est levée si elle n’est pas trouvée.
Lors de l’écriture sous forme textuelle, la valeur est écrite sous la forme de la chaîne correspondante. Si les données de la colonne contiennent des valeurs invalides (des nombres qui ne font pas partie de l’ensemble autorisé), une exception est levée. Lors de la lecture et de l’écriture sous forme binaire, le fonctionnement est le même que pour les types de données Int8 et Int16.
La valeur par défaut implicite est celle qui possède le plus petit nombre.

Lors de `ORDER BY`, `GROUP BY`, `IN`, `DISTINCT`, etc., les Enum se comportent de la même manière que les nombres correspondants. Par exemple, ORDER BY les trie numériquement. Les opérateurs d’égalité et de comparaison fonctionnent de la même manière sur les Enum que sur les valeurs numériques sous-jacentes.

Les valeurs Enum ne peuvent pas être comparées à des nombres. Les Enum peuvent être comparés à une chaîne constante. Si la chaîne utilisée pour la comparaison n’est pas une valeur valide pour l’Enum, une exception est levée. L’opérateur IN est pris en charge avec l’Enum à gauche et un ensemble de chaînes à droite. Les chaînes sont les valeurs de l’Enum correspondant.

La plupart des opérations numériques et sur les chaînes ne sont pas définies pour les valeurs Enum, par exemple ajouter un nombre à un Enum ou concaténer une chaîne à un Enum.
Cependant, l’Enum dispose d’une fonction naturelle `toString` qui renvoie sa valeur de chaîne.

Les valeurs Enum peuvent également être converties en types numériques à l’aide de la fonction `toT`, où T est un type numérique. Lorsque T correspond au type numérique sous-jacent de l’Enum, cette conversion est sans coût.
Le type Enum peut être modifié sans coût à l’aide de ALTER, si seul l’ensemble des valeurs est modifié. Il est possible à la fois d’ajouter et de supprimer des membres de l’Enum à l’aide de ALTER (la suppression n’est sûre que si la valeur supprimée n’a jamais été utilisée dans la table). Par mesure de sécurité, modifier la valeur numérique d’un membre Enum défini précédemment lèvera une exception.

À l’aide de ALTER, il est possible de remplacer un Enum8 par un Enum16 ou inversement, tout comme lorsqu’on remplace un Int8 par un Int16.

<div id="add-enum-values">
  ## AJOUTER DES VALEURS À UN ENUM
</div>

Il existe un raccourci syntaxique pour ajouter de nouvelles valeurs à un enum à l’aide de ALTER [MODIFY COLUMN ADD ENUM VALUES](../../sql-reference/statements/alter/column.md#modify-column-add-enum-values)

```sql
CREATE TABLE enum
(
    x Enum('One' = 1, 'Two', 'Three')
) ENGINE = Memory;
ALTER TABLE enum MODIFY COLUMN x ADD ENUM VALUES ('Zero' = 0, 'Four' = 4);
SHOW CREATE TABLE enum;
```

```text
┌─statement────────────────────────────────────────────────────────────────┐
│CREATE TABLE default.enum                                                 │
│(                                                                         │
│    `x` Enum8('Zero' = 0, 'One' = 1, 'Two' = 2, 'Three' = 3, 'Four' = 4)  │
│)                                                                         │
│ENGINE = Memory                                                           │
└──────────────────────────────────────────────────────────────────────────┘
```