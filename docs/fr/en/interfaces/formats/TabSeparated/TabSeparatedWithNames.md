---
alias: ['TSVWithNames']
description: 'Documentation du format TabSeparatedWithNames'
input_format: true
keywords: ['TabSeparatedWithNames']
output_format: true
slug: /interfaces/formats/TabSeparatedWithNames
title: 'TabSeparatedWithNames'
doc_type: 'reference'
---

| Entrée | Sortie | Alias          |
| ------ | ------ | -------------- |
| ✔      | ✔      | `TSVWithNames` |

<div id="description">
  ## Description
</div>

Diffère du format [`TabSeparated`](./TabSeparated.md) en ceci que les noms des colonnes sont écrits sur la première ligne.

Lors de l’analyse, la première ligne est supposée contenir les noms des colonnes. Vous pouvez utiliser les noms des colonnes pour déterminer leur position et vérifier qu’ils sont corrects.

:::note
Si le paramètre [`input_format_with_names_use_header`](../../../operations/settings/settings-formats.md/#input_format_with_names_use_header) est défini sur `1`,
les colonnes des données d’entrée seront mises en correspondance avec les colonnes de la table à partir de leurs noms, et les colonnes portant des noms inconnus seront ignorées si le paramètre [`input_format_skip_unknown_fields`](../../../operations/settings/settings-formats.md/#input_format_skip_unknown_fields) est défini sur `1`.
Sinon, la première ligne sera ignorée.
:::

<div id="example-usage">
  ## Exemple d’utilisation
</div>

<div id="inserting-data">
  ### Insertion de données
</div>

À l’aide du fichier TSV suivant, nommé `football.tsv` :

```tsv
date    season  home_team       away_team       home_team_goals away_team_goals
2022-04-30      2021    Sutton United   Bradford City   1       4
2022-04-30      2021    Swindon Town    Barrow  2       1
2022-04-30      2021    Tranmere Rovers Oldham Athletic 2       0
2022-05-02      2021    Port Vale       Newport County  1       2
2022-05-02      2021    Salford City    Mansfield Town  2       2
2022-05-07      2021    Barrow  Northampton Town        1       3
2022-05-07      2021    Bradford City   Carlisle United 2       0
2022-05-07      2021    Bristol Rovers  Scunthorpe United       7       0
2022-05-07      2021    Exeter City     Port Vale       0       1
2022-05-07      2021    Harrogate Town A.F.C.   Sutton United   0       2
2022-05-07      2021    Hartlepool United       Colchester United       0       2
2022-05-07      2021    Leyton Orient   Tranmere Rovers 0       1
2022-05-07      2021    Mansfield Town  Forest Green Rovers     2       2
2022-05-07      2021    Newport County  Rochdale        0       2
2022-05-07      2021    Oldham Athletic Crawley Town    3       3
2022-05-07      2021    Stevenage Borough       Salford City    4       2
2022-05-07      2021    Walsall Swindon Town    0       3
```

Insérez les données :

```sql
INSERT INTO football FROM INFILE 'football.tsv' FORMAT TabSeparatedWithNames;
```

<div id="reading-data">
  ### Lire les données
</div>

Lisez les données à l’aide du format `TabSeparatedWithNames` :

```sql
SELECT *
FROM football
FORMAT TabSeparatedWithNames
```

La sortie sera au format TSV :

```tsv
date    season  home_team       away_team       home_team_goals away_team_goals
2022-04-30      2021    Sutton United   Bradford City   1       4
2022-04-30      2021    Swindon Town    Barrow  2       1
2022-04-30      2021    Tranmere Rovers Oldham Athletic 2       0
2022-05-02      2021    Port Vale       Newport County  1       2
2022-05-02      2021    Salford City    Mansfield Town  2       2
2022-05-07      2021    Barrow  Northampton Town        1       3
2022-05-07      2021    Bradford City   Carlisle United 2       0
2022-05-07      2021    Bristol Rovers  Scunthorpe United       7       0
2022-05-07      2021    Exeter City     Port Vale       0       1
2022-05-07      2021    Harrogate Town A.F.C.   Sutton United   0       2
2022-05-07      2021    Hartlepool United       Colchester United       0       2
2022-05-07      2021    Leyton Orient   Tranmere Rovers 0       1
2022-05-07      2021    Mansfield Town  Forest Green Rovers     2       2
2022-05-07      2021    Newport County  Rochdale        0       2
2022-05-07      2021    Oldham Athletic Crawley Town    3       3
2022-05-07      2021    Stevenage Borough       Salford City    4       2
2022-05-07      2021    Walsall Swindon Town    0       3
```

<div id="format-settings">
  ## Paramètres de format
</div>
