---
slug: /sql-reference/statements/create/dictionary/layouts/regexp-tree
title: 'Layout de dictionnaire en arbre d’expressions régulières'
sidebar_label: 'Regexp Tree'
sidebar_position: 12
description: 'Configurer un dictionnaire en arbre d’expressions régulières pour des recherches basées sur des motifs.'
doc_type: 'référence'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<div id="overview">
  ## Vue d’ensemble
</div>

Le dictionnaire `regexp_tree` vous permet d’associer des clés à des valeurs à l’aide de motifs hiérarchiques d’expressions régulières.
Il est optimisé pour les recherches par correspondance de motifs (par exemple, pour classer des chaînes telles que les chaînes d&#39;user agent en faisant correspondre des expressions régulières), plutôt que pour une correspondance exacte des clés.

<iframe width="1024" height="576" src="https://www.youtube.com/embed/ESlAhUJMoz8?si=sY2OVm-zcuxlDRaX" title="Introduction aux dictionnaires arbre d’expressions régulières de ClickHouse" frameborder="0" allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture; web-share" referrerpolicy="strict-origin-when-cross-origin" allowfullscreen />

<div id="use-regular-expression-tree-dictionary-in-clickhouse-open-source">
  ## Utiliser le dictionnaire en arbre d’expressions régulières avec la source YAMLRegExpTree
</div>

<CloudNotSupportedBadge />

Les dictionnaires en arbre d’expressions régulières sont définis dans ClickHouse open-source à l’aide de la source [`YAMLRegExpTree`](../sources/yamlregexptree.md), à laquelle on fournit le chemin d’un fichier YAML contenant l’arbre d’expressions régulières.

```sql title="Query"
CREATE DICTIONARY regexp_dict
(
    regexp String,
    name String,
    version String
)
PRIMARY KEY(regexp)
SOURCE(YAMLRegExpTree(PATH '/var/lib/clickhouse/user_files/regexp_tree.yaml'))
LAYOUT(regexp_tree)
...
```

La source de dictionnaire [`YAMLRegExpTree`](../sources/yamlregexptree.md) représente la structure d’un arbre d’expressions régulières. Par exemple :

```yaml
- regexp: 'Linux/(\d+[\.\d]*).+tlinux'
  name: 'TencentOS'
  version: '\1'

- regexp: '\d+/tclwebkit(?:\d+[\.\d]*)'
  name: 'Android'
  versions:
    - regexp: '33/tclwebkit'
      version: '13'
    - regexp: '3[12]/tclwebkit'
      version: '12'
    - regexp: '30/tclwebkit'
      version: '11'
    - regexp: '29/tclwebkit'
      version: '10'
```

Cette configuration se compose d’une liste de nœuds d’arbre d’expressions régulières. Chaque nœud a la structure suivante :

* **regexp** : l’expression régulière du nœud.
* **attributes** : une liste d’attributs de dictionnaire définis par l’utilisateur. Dans cet exemple, il y a deux attributs : `name` et `version`. Le premier nœud définit les deux attributs. Le second nœud définit uniquement l’attribut `name`. L’attribut `version` est fourni par les nœuds enfants du second nœud.
  * La valeur d’un attribut peut contenir des **back-references**, qui renvoient à des groupes de capture de l’expression régulière correspondante. Dans l’exemple, la valeur de l’attribut `version` dans le premier nœud se compose d’une back-reference `\1` vers le groupe de capture `(\d+[\.\d]*)` dans l’expression régulière. Les numéros de back-reference vont de 1 à 9 et s’écrivent `$1` ou `\1` (pour le numéro 1). La back-reference est remplacée par le groupe de capture correspondant pendant l’exécution de la requête.
* **child nodes** : une liste d’enfants d’un nœud d’arbre d’expressions régulières, chacun ayant ses propres attributs et, éventuellement, ses propres nœuds enfants. La correspondance des chaînes s’effectue en parcours en profondeur. Si une chaîne correspond à un nœud regexp, le dictionnaire vérifie si elle correspond également aux nœuds enfants de ce nœud. Si c’est le cas, les attributs du nœud correspondant le plus profond sont affectés. Les attributs d’un nœud enfant écrasent les attributs de même nom des nœuds parents. Le nom des nœuds enfants dans les fichiers YAML peut être arbitraire, par exemple `versions` dans l’exemple ci-dessus.

Les dictionnaires en arbre d’expressions régulières autorisent l’accès uniquement via les fonctions `dictGet`, `dictGetOrDefault` et `dictGetAll`. Par exemple :

```sql title="Query"
SELECT dictGet('regexp_dict', ('name', 'version'), '31/tclwebkit1024');
```

```text title="Response"
┌─dictGet('regexp_dict', ('name', 'version'), '31/tclwebkit1024')─┐
│ ('Android','12')                                                │
└─────────────────────────────────────────────────────────────────┘
```

Dans ce cas, nous faisons d’abord correspondre l’expression régulière `\d+/tclwebkit(?:\d+[\.\d]*)` dans le deuxième nœud de la couche supérieure.
Le dictionnaire poursuit ensuite l’examen des nœuds enfants et constate que la chaîne correspond également à `3[12]/tclwebkit`.
Par conséquent, la valeur de l’attribut `name` est `Android` (définie dans la première couche) et la valeur de l’attribut `version` est `12` (définie dans le nœud enfant).

Avec un fichier de configuration YAML sophistiqué, vous pouvez utiliser des dictionnaires en arbre d’expressions régulières comme analyseur de chaîne user agent.
ClickHouse prend en charge [uap-core](https://github.com/ua-parser/uap-core), et vous pouvez voir comment l’utiliser dans le test fonctionnel [02504&#95;regexp&#95;dictionary&#95;ua&#95;parser](https://github.com/ClickHouse/ClickHouse/blob/master/tests/queries/0_stateless/02504_regexp_dictionary_ua_parser.sh)

<div id="collecting-attribute-values">
  ### Récupération des valeurs d’attribut
</div>

Il est parfois utile de renvoyer les valeurs de plusieurs expressions régulières correspondantes, plutôt que seulement celle d’un nœud feuille. Dans ce cas, on peut utiliser la fonction spécialisée [`dictGetAll`](/fr/sql-reference/functions/ext-dict-functions.md#dictGetAll). Si un nœud a une valeur d’attribut de type `T`, `dictGetAll` renverra un `Array(T)` contenant zéro, une ou plusieurs valeurs.

Par défaut, le nombre de correspondances renvoyées par clé n’est pas limité. Il est possible de définir une limite en la passant comme quatrième argument optionnel à `dictGetAll`. Le tableau est rempli dans un *ordre topologique*, ce qui signifie que les nœuds enfants précèdent les nœuds parents et que les nœuds frères suivent l’ordre défini dans la source.

Exemple :

```sql
CREATE DICTIONARY regexp_dict
(
    regexp String,
    tag String,
    topological_index Int64,
    captured Nullable(String),
    parent String
)
PRIMARY KEY(regexp)
SOURCE(YAMLRegExpTree(PATH '/var/lib/clickhouse/user_files/regexp_tree.yaml'))
LAYOUT(regexp_tree)
LIFETIME(0)
```

```yaml
# /var/lib/clickhouse/user_files/regexp_tree.yaml
- regexp: 'clickhouse\.com'
  tag: 'ClickHouse'
  topological_index: 1
  paths:
    - regexp: 'clickhouse\.com/docs(.*)'
      tag: 'ClickHouse Documentation'
      topological_index: 0
      captured: '\1'
      parent: 'ClickHouse'

- regexp: '/docs(/|$)'
  tag: 'Documentation'
  topological_index: 2

- regexp: 'github.com'
  tag: 'GitHub'
  topological_index: 3
  captured: 'NULL'
```

```sql title="Query"
CREATE TABLE urls (url String) ENGINE=MergeTree ORDER BY url;
INSERT INTO urls VALUES ('clickhouse.com'), ('clickhouse.com/docs/en'), ('github.com/clickhouse/tree/master/docs');
SELECT url, dictGetAll('regexp_dict', ('tag', 'topological_index', 'captured', 'parent'), url, 2) FROM urls;
```

```text title="Response"
┌─url────────────────────────────────────┬─dictGetAll('regexp_dict', ('tag', 'topological_index', 'captured', 'parent'), url, 2)─┐
│ clickhouse.com                         │ (['ClickHouse'],[1],[],[])                                                            │
│ clickhouse.com/docs/en                 │ (['ClickHouse Documentation','ClickHouse'],[0,1],['/en'],['ClickHouse'])              │
│ github.com/clickhouse/tree/master/docs │ (['Documentation','GitHub'],[2,3],[NULL],[])                                          │
└────────────────────────────────────────┴───────────────────────────────────────────────────────────────────────────────────────┘
```

<div id="matching-modes">
  ### Modes de correspondance
</div>

Le comportement de correspondance des motifs peut être modifié à l’aide de certains paramètres du dictionnaire :

* `regexp_dict_flag_case_insensitive` : Active une correspondance insensible à la casse (par défaut, `false`). Peut être redéfini dans des expressions individuelles avec `(?i)` et `(?-i)`.
* `regexp_dict_flag_dotall` : Permet à `.` de correspondre aux caractères de saut de ligne (par défaut, `false`).

<div id="use-regular-expression-tree-dictionary-in-clickhouse-cloud">
  ## Utiliser un dictionnaire en arbre d’expressions régulières dans ClickHouse Cloud
</div>

La source [`YAMLRegExpTree`](../sources/yamlregexptree.md) fonctionne dans ClickHouse Open Source, mais pas dans ClickHouse Cloud.
Pour utiliser des dictionnaires en arbre d’expressions régulières dans ClickHouse Cloud, créez d’abord localement, dans ClickHouse Open Source, un dictionnaire en arbre d’expressions régulières à partir d’un fichier YAML, puis exportez ce dictionnaire vers un fichier CSV à l’aide de la fonction de table `dictionary` et de la clause [INTO OUTFILE](/fr/sql-reference/statements/select/into-outfile.md).

```sql
SELECT * FROM dictionary(regexp_dict) INTO OUTFILE('regexp_dict.csv')
```

Le contenu du fichier CSV est :

```text
1,0,"Linux/(\d+[\.\d]*).+tlinux","['version','name']","['\\1','TencentOS']"
2,0,"(\d+)/tclwebkit(\d+[\.\d]*)","['comment','version','name']","['test $1 and $2','$1','Android']"
3,2,"33/tclwebkit","['version']","['13']"
4,2,"3[12]/tclwebkit","['version']","['12']"
5,2,"3[12]/tclwebkit","['version']","['11']"
6,2,"3[12]/tclwebkit","['version']","['10']"
```

Le schéma du fichier exporté est :

* `id UInt64` : l’identifiant du nœud RegexpTree.
* `parent_id UInt64` : l’identifiant du parent d’un nœud.
* `regexp String` : la chaîne de l’expression régulière.
* `keys Array(String)` : les noms des attributs définis par l’utilisateur.
* `values Array(String)` : les valeurs des attributs définis par l’utilisateur.

Pour créer le dictionnaire dans ClickHouse Cloud, créez d’abord une table `regexp_dictionary_source_table` avec la structure de table ci-dessous :

```sql
CREATE TABLE regexp_dictionary_source_table
(
    id UInt64,
    parent_id UInt64,
    regexp String,
    keys   Array(String),
    values Array(String)
) ENGINE=Memory;
```

Ensuite, mettez à jour le fichier CSV local avec

```bash
clickhouse client \
    --host MY_HOST \
    --secure \
    --password MY_PASSWORD \
    --query "
    INSERT INTO regexp_dictionary_source_table
    SELECT * FROM input ('id UInt64, parent_id UInt64, regexp String, keys Array(String), values Array(String)')
    FORMAT CSV" < regexp_dict.csv
```

Consultez [Insérer des fichiers locaux](/fr/integrations/data-ingestion/insert-local-files) pour plus de détails. Après avoir initialisé la table source, nous pouvons créer un RegexpTree à partir de celle-ci :

```sql
CREATE DICTIONARY regexp_dict
(
    regexp String,
    name String,
    version String
PRIMARY KEY(regexp)
SOURCE(CLICKHOUSE(TABLE 'regexp_dictionary_source_table'))
LIFETIME(0)
LAYOUT(regexp_tree);
```