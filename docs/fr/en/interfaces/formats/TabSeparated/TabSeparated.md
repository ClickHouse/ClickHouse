---
alias: ['TSV']
description: 'Documentation sur le format TSV'
input_format: true
keywords: ['TabSeparated', 'TSV']
output_format: true
slug: /interfaces/formats/TabSeparated
title: 'TabSeparated'
doc_type: 'reference'
---

| Entrée | Sortie | Alias |
| ------ | ------ | ----- |
| ✔      | ✔      | `TSV` |

<div id="description">
  ## Description
</div>

Au format TabSeparated, les données sont écrites ligne par ligne. Chaque ligne contient des valeurs séparées par des tabulations. Chaque valeur est suivie d’une tabulation, sauf la dernière valeur de la ligne, qui est suivie d’un saut de ligne. Les sauts de ligne sont toujours au format Unix. La dernière ligne doit également se terminer par un saut de ligne. Les valeurs sont écrites au format texte, sans guillemets, et les caractères spéciaux sont échappés.

Ce format est également disponible sous le nom `TSV`.

Le format `TabSeparated` est pratique pour traiter des données à l’aide de programmes et de scripts personnalisés. Il est utilisé par défaut dans l’interface HTTP et dans le mode batch du client en ligne de commande. Ce format permet également de transférer des données entre différents SGBD. Par exemple, vous pouvez récupérer un dump depuis MySQL et le charger dans ClickHouse, ou inversement.

Le format `TabSeparated` prend également en charge l’affichage des valeurs totales (lors de l’utilisation de WITH TOTALS) et des valeurs extrêmes (lorsque &#39;extremes&#39; est défini sur 1). Dans ces cas, les valeurs totales et les valeurs extrêmes sont affichées après les données principales. Le résultat principal, les valeurs totales et les valeurs extrêmes sont séparés par une ligne vide. Exemple :

```sql
SELECT EventDate, count() AS c FROM test.hits GROUP BY EventDate WITH TOTALS ORDER BY EventDate FORMAT TabSeparated

2014-03-17      1406958
2014-03-18      1383658
2014-03-19      1405797
2014-03-20      1353623
2014-03-21      1245779
2014-03-22      1031592
2014-03-23      1046491

1970-01-01      8873898

2014-03-17      1031592
2014-03-23      1406958
```

<div id="tabseparated-data-formatting">
  ## Formatage des données
</div>

Les nombres entiers s’écrivent sous forme décimale. Ils peuvent contenir un caractère `+` supplémentaire au début (ignoré lors du parsing et non conservé lors du formatage). Les nombres non négatifs ne peuvent pas contenir de signe moins. À la lecture, une chaîne vide peut être interprétée comme zéro ou, pour les types signés, une chaîne constituée uniquement d’un signe moins peut également être interprétée comme zéro. Les nombres qui ne tiennent pas dans le Type de donnée correspondant peuvent être interprétés comme une autre valeur, sans message d’erreur.

Les nombres à virgule flottante s’écrivent sous forme décimale. Le point est utilisé comme séparateur décimal. Les écritures exponentielles sont prises en charge, ainsi que `inf`, `+inf`, `-inf` et `nan`. Une valeur de type flottant peut commencer ou se terminer par un point décimal.
Lors du formatage, une perte de précision peut se produire pour les nombres à virgule flottante.
Lors du parsing, il n’est pas strictement nécessaire de lire la valeur représentable par la machine la plus proche.

Les Dates s’écrivent au format YYYY-MM-DD et sont interprétées selon ce même format, avec toutefois n’importe quels caractères comme séparateurs.
Les dates avec heure s’écrivent au format `YYYY-MM-DD hh:mm:ss` et sont interprétées selon ce même format, avec toutefois n’importe quels caractères comme séparateurs.
Tout cela se fait dans le fuseau horaire du système au moment du démarrage du client ou du serveur (selon celui qui formate les données). Pour les dates avec heure, l’heure d’été n’est pas spécifiée. Ainsi, si un dump contient des heures pendant la période d’heure d’été, il ne correspond pas aux données de manière non ambiguë, et le parsing choisira l’une des deux heures.
Lors d’une opération de lecture, les dates invalides et les dates avec heure peuvent être interprétées avec un dépassement naturel ou comme des dates et heures nulles, sans message d’erreur.

Exceptionnellement, le parsing des dates avec heure est également pris en charge au format Unix timestamp, s’il se compose d’exactement 10 chiffres décimaux. Le résultat ne dépend pas du fuseau horaire. Les formats `YYYY-MM-DD hh:mm:ss` et `NNNNNNNNNN` sont différenciés automatiquement.

Les chaînes sont produites avec les caractères spéciaux échappés par une barre oblique inverse. Les séquences d’échappement suivantes sont utilisées en sortie : `\b`, `\f`, `\r`, `\n`, `\t`, `\0`, `\'`, `\\`. Le parsing prend également en charge les séquences `\a`, `\v` et `\xHH` (séquences d’échappement hexadécimales), ainsi que toute séquence `\c`, où `c` est n’importe quel caractère (ces séquences sont converties en `c`). Ainsi, la lecture des données prend en charge les formats dans lesquels un saut de ligne peut être écrit sous la forme `\n`, `\`, ou d’un saut de ligne réel. Par exemple, la chaîne `Hello world` avec un saut de ligne entre les mots à la place d’un espace peut être interprétée dans n’importe laquelle des variantes suivantes :

```text
Hello\nworld

Hello\
world
```

La deuxième variante est prise en charge, car MySQL l’utilise lorsqu’il écrit des dumps au format tabulé.

L’ensemble minimal de caractères que vous devez échapper lors de la transmission de données au format TabSeparated : tabulation, saut de ligne (LF) et backslash.

Seul un petit nombre de symboles sont échappés. Vous pouvez facilement tomber sur une chaîne que votre terminal affichera incorrectement en sortie.

Les tableaux s’écrivent sous la forme d’une liste de valeurs séparées par des virgules entre `[]`. Les éléments numériques du tableau sont formatés normalement. Les types `Date` et `DateTime` sont écrits entre guillemets simples. Les chaînes sont écrites entre guillemets simples, avec les mêmes règles d’échappement que ci-dessus.

[NULL](/fr/sql-reference/syntax.md) est formaté selon le paramètre [format&#95;tsv&#95;null&#95;representation](/fr/operations/settings/settings-formats.md/#format_tsv_null_representation) (la valeur par défaut est `\N`).

Dans les données d’entrée, les valeurs ENUM peuvent être représentées sous forme de noms ou d’identifiants. Nous essayons d’abord d’associer la valeur d’entrée au nom ENUM. En cas d’échec, si la valeur d’entrée est un nombre, nous essayons d’associer ce nombre à l’identifiant ENUM.
Si les données d’entrée ne contiennent que des identifiants ENUM, il est recommandé d’activer le paramètre [input&#95;format&#95;tsv&#95;enum&#95;as&#95;number](/fr/operations/settings/settings-formats.md/#input_format_tsv_enum_as_number) pour optimiser l’analyse des ENUM.

Chaque élément des structures [Nested](/fr/sql-reference/data-types/nested-data-structures/index.md) est représenté sous forme de tableau.

Par exemple :

```sql
CREATE TABLE nestedt
(
    `id` UInt8,
    `aux` Nested(
        a UInt8,
        b String
    )
)
ENGINE = TinyLog
```

```sql
INSERT INTO nestedt VALUES ( 1, [1], ['a'])
```

```sql
SELECT * FROM nestedt FORMAT TSV
```

```response
1  [1]    ['a']
```

<div id="example-usage">
  ## Exemple d’utilisation
</div>

<div id="inserting-data">
  ### Insérer des données
</div>

À l’aide du fichier TSV suivant, nommé `football.tsv` :

```tsv
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
INSERT INTO football FROM INFILE 'football.tsv' FORMAT TabSeparated;
```

<div id="reading-data">
  ### Lecture des données
</div>

Lisez les données au format `TabSeparated` :

```sql
SELECT *
FROM football
FORMAT TabSeparated
```

La sortie sera au format tabulé :

```tsv
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

| Paramètre                                                                                                                                                | Description                                                                                                                                                                                                                                                                                       | Par défaut |
| -------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------- |
| [`format_tsv_null_representation`](/fr/operations/settings/settings-formats.md/#format_tsv_null_representation)                                             | Représentation NULL personnalisée pour le format TSV.                                                                                                                                                                                                                                             | `\N`       |
| [`input_format_tsv_empty_as_default`](/fr/operations/settings/settings-formats.md/#input_format_tsv_empty_as_default)                                       | traite les champs vides dans l&#39;entrée TSV comme des valeurs par défaut. Pour les expressions par défaut complexes, [input&#95;format&#95;defaults&#95;for&#95;omitted&#95;fields](/fr/operations/settings/settings-formats.md/#input_format_defaults_for_omitted_fields) doit aussi être activé. | `false`    |
| [`input_format_tsv_enum_as_number`](/fr/operations/settings/settings-formats.md/#input_format_tsv_enum_as_number)                                           | traite les valeurs enum insérées dans les formats TSV comme des indices enum.                                                                                                                                                                                                                     | `false`    |
| [`input_format_tsv_use_best_effort_in_schema_inference`](/fr/operations/settings/settings-formats.md/#input_format_tsv_use_best_effort_in_schema_inference) | utilise certains ajustements et heuristiques pour inférer le schéma du format TSV. Si cette option est désactivée, tous les champs seront inférés comme des Strings.                                                                                                                              | `true`     |
| [`output_format_tsv_crlf_end_of_line`](/fr/operations/settings/settings-formats.md/#output_format_tsv_crlf_end_of_line)                                     | si cette option est définie sur true, la fin de ligne du format de sortie TSV sera `\r\n` au lieu de `\n`.                                                                                                                                                                                        | `false`    |
| [`input_format_tsv_crlf_end_of_line`](/fr/operations/settings/settings-formats.md/#input_format_tsv_crlf_end_of_line)                                       | si cette option est définie sur true, la fin de ligne du format d&#39;entrée TSV sera `\r\n` au lieu de `\n`.                                                                                                                                                                                     | `false`    |
| [`input_format_tsv_skip_first_lines`](/fr/operations/settings/settings-formats.md/#input_format_tsv_skip_first_lines)                                       | ignore le nombre spécifié de lignes au début des données.                                                                                                                                                                                                                                         | `0`        |
| [`input_format_tsv_detect_header`](/fr/operations/settings/settings-formats.md/#input_format_tsv_detect_header)                                             | détecte automatiquement l&#39;en-tête contenant les noms et les types dans le format TSV.                                                                                                                                                                                                         | `true`     |
| [`input_format_tsv_skip_trailing_empty_lines`](/fr/operations/settings/settings-formats.md/#input_format_tsv_skip_trailing_empty_lines)                     | ignore les lignes vides finales à la fin des données.                                                                                                                                                                                                                                             | `false`    |
| [`input_format_tsv_allow_variable_number_of_columns`](/fr/operations/settings/settings-formats.md/#input_format_tsv_allow_variable_number_of_columns)       | autorise un nombre variable de colonnes dans le format TSV, ignore les colonnes supplémentaires et utilise des valeurs par défaut pour les colonnes manquantes.                                                                                                                                   | `false`    |