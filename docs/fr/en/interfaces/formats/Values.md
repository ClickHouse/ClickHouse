---
alias: []
description: 'Documentation sur le format Values'
input_format: true
keywords: ['Values']
output_format: true
slug: /interfaces/formats/Values
title: 'Values'
doc_type: 'guide'
---

| Entrée | Sortie | Alias |
| ------ | ------ | ----- |
| ✔      | ✔      |       |

<div id="description">
  ## Description
</div>

Le format `Values` affiche chaque ligne entre parenthèses.

* Les lignes sont séparées par des virgules, sans virgule après la dernière.
* Les valeurs à l&#39;intérieur des parenthèses sont également séparées par des virgules.
* Les nombres sont affichés au format décimal, sans guillemets.
* Les tableaux sont affichés dans `[]`.
* Les chaînes, les dates et les dates avec heure sont affichées entre guillemets.
* Les règles d&#39;échappement et l&#39;analyse sont similaires à celles du format [TabSeparated](TabSeparated/TabSeparated.md).

Lors du formatage, aucun espace supplémentaire n&#39;est inséré, mais lors de l&#39;analyse, les espaces sont autorisés et ignorés (sauf à l&#39;intérieur des valeurs de tableau, où ils ne sont pas autorisés).
[`NULL`](/fr/sql-reference/syntax.md) est représenté par `NULL`.

Ensemble minimal de caractères à échapper lorsque vous transmettez des données au format `Values` :

* guillemets simples
* barres obliques inverses

C&#39;est le format utilisé dans `INSERT INTO t VALUES ...`, mais vous pouvez aussi l&#39;utiliser pour mettre en forme le résultat de la requête.

<div id="example-usage">
  ## Exemple d’utilisation
</div>

<div id="inserting-data">
  ### Insertion de données
</div>

Le format `Values` est celui utilisé par `INSERT`, donc toute instruction `INSERT ... VALUES`
l’utilise déjà. La clause `FORMAT Values` peut être indiquée explicitement, et les
lignes peuvent être fournies depuis un flux ou un fichier. Chaque ligne est un
tuple entre parenthèses dont les éléments sont séparés par des virgules, les tuples
eux-mêmes étant séparés par des virgules :

```sql title="Query"
CREATE TABLE t (id UInt32, name String, values Array(UInt32)) ENGINE = Memory;

INSERT INTO t FORMAT Values (1, 'a', [10, 20]), (2, 'b', [30]);

SELECT * FROM t ORDER BY id;
```

```response title="Response"
┌─id─┬─name─┬─values──┐
│  1 │ a    │ [10,20] │
│  2 │ b    │ [30]    │
└────┴──────┴─────────┘
```

<div id="using-expressions">
  ### Utilisation d&#39;expressions sur les données d&#39;entrée
</div>

Contrairement à la plupart des formats d&#39;entrée, `Values` peut évaluer des expressions SQL dans chaque champ,
au lieu d&#39;accepter uniquement des littéraux. Ce comportement est contrôlé par
[`input_format_values_interpret_expressions`](#format-settings) (activé par
défaut) : lorsqu&#39;un champ ne peut pas être lu par le parseur en streaming rapide, ClickHouse
bascule vers le parseur SQL et interprète le champ comme une expression.

```sql title="Query"
CREATE TABLE prices (item String, total UInt32) ENGINE = Memory;

INSERT INTO prices FORMAT Values ('apple', 3 * 4), ('pear', length('hello') + 10);

SELECT * FROM prices ORDER BY total;
```

```response title="Response"
┌─item──┬─total─┐
│ apple │    12 │
│ pear  │    15 │
└───────┴───────┘
```

<div id="selecting-data">
  ### Sélection des données
</div>

Le format `Values` peut également être utilisé pour formater les résultats de la requête. Les nombres sont
écrits sans guillemets, les tableaux entre `[]`, et les chaînes ainsi que les dates entre guillemets simples ;
les guillemets simples et les barres obliques inverses à l&#39;intérieur des chaînes sont échappés avec une barre oblique inversée, et
[`NULL`](/fr/sql-reference/syntax.md) s&#39;écrit `NULL` :

```sql title="Query"
SELECT 1 AS a, 'O''Reilly' AS b, NULL::Nullable(String) AS c FORMAT Values;
```

```response title="Response"
(1,'O\'Reilly',NULL)
```

<div id="format-settings">
  ## Paramètres de format
</div>

| Paramètre                                                                                                                                                   | Description                                                                                                                                                                                                                                 | Par défaut |
| ----------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------- |
| [`input_format_values_interpret_expressions`](../../operations/settings/settings-formats.md/#input_format_values_interpret_expressions)                     | si le champ ne peut pas être analysé par le parseur en streaming, exécute le parseur SQL et tente de l’interpréter comme une expression SQL.                                                                                                | `true`     |
| [`input_format_values_deduce_templates_of_expressions`](../../operations/settings/settings-formats.md/#input_format_values_deduce_templates_of_expressions) | si le champ ne peut pas être analysé par le parseur en streaming, exécute le parseur SQL, déduit le modèle de l’expression SQL, tente d’analyser toutes les lignes à l’aide du modèle, puis interprète l’expression pour toutes les lignes. | `true`     |
| [`input_format_values_accurate_types_of_literals`](../../operations/settings/settings-formats.md/#input_format_values_accurate_types_of_literals)           | lors de l’analyse et de l’interprétation des expressions à l’aide du modèle, vérifie le type réel du littéral afin d’éviter d’éventuels problèmes de dépassement et de précision.                                                           | `true`     |