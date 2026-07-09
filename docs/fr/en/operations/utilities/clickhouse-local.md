---
description: 'Guide d’utilisation de clickhouse-local pour traiter des données sans utiliser de serveur'
sidebar_label: 'clickhouse-local'
sidebar_position: 60
slug: /operations/utilities/clickhouse-local
title: 'clickhouse-local'
doc_type: 'reference'
---

<div id="when-to-use-clickhouse-local-vs-clickhouse">
  ## Quand utiliser clickhouse-local plutôt que ClickHouse
</div>

`clickhouse-local` est une version de ClickHouse facile à utiliser, idéale pour les développeurs qui ont besoin de traiter rapidement des fichiers locaux et distants à l’aide de SQL, sans avoir à installer un serveur de base de données complet. Avec `clickhouse-local`, les développeurs peuvent exécuter des commandes SQL (à l’aide du [dialecte SQL ClickHouse](../../sql-reference/index.md)) directement depuis la ligne de commande, ce qui offre un moyen simple et efficace d’accéder aux fonctionnalités de ClickHouse sans avoir à installer ClickHouse dans son intégralité. L’un des principaux avantages de `clickhouse-local` est qu’il est déjà inclus lors de l’installation de [clickhouse-client](/fr/operations/utilities/clickhouse-local). Les développeurs peuvent ainsi prendre rapidement en main `clickhouse-local`, sans processus d’installation complexe.

Bien que `clickhouse-local` soit un excellent outil pour le développement, les tests et le traitement de fichiers, il ne convient pas pour servir des utilisateurs finaux ou des applications. Dans ces cas, il est recommandé d’utiliser [ClickHouse](/fr/install) open source. ClickHouse est une puissante base de données OLAP conçue pour gérer des charges de travail analytiques à grande échelle. Elle permet de traiter rapidement et efficacement des requêtes complexes sur de grands jeux de données, ce qui en fait une solution idéale pour les environnements de production où de hautes performances sont essentielles. En outre, ClickHouse offre un large éventail de fonctionnalités, telles que la réplication, le sharding et la haute disponibilité, essentielles pour passer à l’échelle afin de gérer de grands jeux de données et de servir des applications. Si vous devez gérer des jeux de données plus volumineux ou servir des utilisateurs finaux ou des applications, nous vous recommandons d’utiliser ClickHouse open source plutôt que `clickhouse-local`.

Veuillez consulter la documentation ci-dessous, qui présente des cas d’usage de `clickhouse-local`, par exemple [interroger un fichier local](#query_data_in_file) ou [lire un fichier Parquet dans S3](#query-data-in-a-parquet-file-in-aws-s3).

<div id="download-clickhouse-local">
  ## Télécharger clickhouse-local
</div>

`clickhouse-local` utilise le même binaire `clickhouse` que le serveur ClickHouse et `clickhouse-client`. Le moyen le plus simple de télécharger la dernière version est d’utiliser la commande suivante :

```bash
curl https://clickhouse.com/ | sh
```

:::note
Le binaire que vous venez de télécharger peut exécuter toutes sortes d’outils et d’utilitaires ClickHouse. Si vous souhaitez utiliser ClickHouse en tant que serveur de base de données, consultez le [Quick Start](/fr/get-started/quick-start).
:::

<div id="query_data_in_file">
  ## Interroger des données dans un fichier avec SQL
</div>

Une utilisation courante de `clickhouse-local` consiste à exécuter des requêtes ad hoc sur des fichiers, sans avoir à insérer les données dans une table. `clickhouse-local` peut lire les données d’un fichier dans une table temporaire et exécuter votre SQL.

Si le fichier se trouve sur la même machine que `clickhouse-local`, vous pouvez simplement spécifier le fichier à charger. Le fichier `reviews.tsv` suivant contient un échantillon d’avis sur des produits Amazon :

```bash
./clickhouse local -q "SELECT * FROM 'reviews.tsv'"
```

Cette commande est l’équivalent de :

```bash
./clickhouse local -q "SELECT * FROM file('reviews.tsv')"
```

ClickHouse déduit de l’extension du nom de fichier que le fichier utilise un format séparé par des tabulations. Si vous devez spécifier explicitement le format, ajoutez simplement l’un des [nombreux formats d’entrée de ClickHouse](../../interfaces/formats.md) :

```bash
./clickhouse local -q "SELECT * FROM file('reviews.tsv', 'TabSeparated')"
```

La fonction de table `file` crée une table et vous pouvez utiliser `DESCRIBE` pour afficher le schéma inféré :

```bash
./clickhouse local -q "DESCRIBE file('reviews.tsv')"
```

:::tip
Vous pouvez utiliser des globs dans les noms de fichiers (voir les [substitutions de globs](/fr/sql-reference/table-functions/file.md/#globs-in-path)).

Exemples :

```bash
./clickhouse local -q "SELECT * FROM 'reviews*.jsonl'"
./clickhouse local -q "SELECT * FROM 'review_?.csv'"
./clickhouse local -q "SELECT * FROM 'review_{1..3}.csv'"
```

:::

```response
marketplace    Nullable(String)
customer_id    Nullable(Int64)
review_id    Nullable(String)
product_id    Nullable(String)
product_parent    Nullable(Int64)
product_title    Nullable(String)
product_category    Nullable(String)
star_rating    Nullable(Int64)
helpful_votes    Nullable(Int64)
total_votes    Nullable(Int64)
vine    Nullable(String)
verified_purchase    Nullable(String)
review_headline    Nullable(String)
review_body    Nullable(String)
review_date    Nullable(Date)
```

Trouvons le produit le mieux noté :

```bash
./clickhouse local -q "SELECT
    argMax(product_title,star_rating),
    max(star_rating)
FROM file('reviews.tsv')"
```

```response
Monopoly Junior Board Game    5
```

<div id="query-data-in-a-parquet-file-in-aws-s3">
  ## Interroger les données d’un fichier Parquet dans AWS S3
</div>

Si vous avez un fichier dans S3, utilisez `clickhouse-local` et la fonction de table `s3` pour interroger le fichier sur place (sans insérer les données dans une table ClickHouse). Nous avons un fichier nommé `house_0.parquet` dans un bucket public qui contient les prix des biens immobiliers vendus au Royaume-Uni. Voyons combien de lignes il contient :

```bash
./clickhouse local -q "
SELECT count()
FROM s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/house_parquet/house_0.parquet')"
```

Le fichier contient 2,7 M lignes :

```response
2772030
```

Il est toujours utile de voir le schéma inféré que ClickHouse déduit du fichier :

```bash
./clickhouse local -q "DESCRIBE s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/house_parquet/house_0.parquet')"
```

```response
price    Nullable(Int64)
date    Nullable(UInt16)
postcode1    Nullable(String)
postcode2    Nullable(String)
type    Nullable(String)
is_new    Nullable(UInt8)
duration    Nullable(String)
addr1    Nullable(String)
addr2    Nullable(String)
street    Nullable(String)
locality    Nullable(String)
town    Nullable(String)
district    Nullable(String)
county    Nullable(String)
```

Voyons quels sont les quartiers les plus chers :

```bash
./clickhouse local -q "
SELECT
    town,
    district,
    count() AS c,
    round(avg(price)) AS price,
    bar(price, 0, 5000000, 100)
FROM s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/house_parquet/house_0.parquet')
GROUP BY
    town,
    district
HAVING c >= 100
ORDER BY price DESC
LIMIT 10"
```

```response
LONDON    CITY OF LONDON    886    2271305    █████████████████████████████████████████████▍
LEATHERHEAD    ELMBRIDGE    206    1176680    ███████████████████████▌
LONDON    CITY OF WESTMINSTER    12577    1108221    ██████████████████████▏
LONDON    KENSINGTON AND CHELSEA    8728    1094496    █████████████████████▉
HYTHE    FOLKESTONE AND HYTHE    130    1023980    ████████████████████▍
CHALFONT ST GILES    CHILTERN    113    835754    ████████████████▋
AMERSHAM    BUCKINGHAMSHIRE    113    799596    ███████████████▉
VIRGINIA WATER    RUNNYMEDE    356    789301    ███████████████▊
BARNET    ENFIELD    282    740514    ██████████████▊
NORTHWOOD    THREE RIVERS    184    731609    ██████████████▋
```

:::tip
Lorsque vous êtes prêt à importer vos fichiers dans ClickHouse, démarrez un serveur ClickHouse et insérez les résultats de vos fonctions de table `file` et `s3` dans une table `MergeTree`. Consultez le [Quick Start](/fr/get-started/quick-start) pour plus de détails.
:::

<div id="format-conversions">
  ## Conversions de formats
</div>

Vous pouvez utiliser `clickhouse-local` pour convertir des données d’un format à un autre. Exemple :

```bash
$ clickhouse-local --input-format JSONLines --output-format CSV --query "SELECT * FROM table" < data.json > data.csv
```

Les formats sont détectés automatiquement à partir des extensions de fichier :

```bash
$ clickhouse-local --query "SELECT * FROM table" < data.json > data.csv
```

Pour faire plus court, vous pouvez l’écrire avec l’argument `--copy` :

```bash
$ clickhouse-local --copy < data.json > data.csv
```

<div id="usage">
  ## Utilisation
</div>

Par défaut, `clickhouse-local` a accès aux données d’un serveur ClickHouse sur le même hôte et ne dépend pas de la configuration du serveur. Il permet également de charger la configuration du serveur à l’aide de l’argument `--config-file`. Pour les données temporaires, un répertoire temporaire de données unique est créé par défaut.

Utilisation de base (Linux) :

```bash
$ clickhouse-local --structure "table_structure" --input-format "format_of_incoming_data" --query "query"
```

Utilisation de base (Mac) :

```bash
$ ./clickhouse local --structure "table_structure" --input-format "format_of_incoming_data" --query "query"
```

:::note
`clickhouse-local` est également pris en charge sur Windows via WSL2.
:::

Arguments :

* `-S`, `--structure` — structure de la table pour les données d&#39;entrée.
* `--input-format` — format d&#39;entrée, `TSV` par défaut.
* `-F`, `--file` — chemin des données, `stdin` par défaut.
* `-q`, `--query` — requêtes à exécuter, avec `;` comme délimiteur. `--query` peut être spécifié plusieurs fois, par ex. `--query "SELECT 1" --query "SELECT 2"`. Ne peut pas être utilisé simultanément avec `--queries-file`.
* `--queries-file` - chemin du fichier contenant les requêtes à exécuter. `--queries-file` peut être spécifié plusieurs fois, par ex. `--query queries1.sql --query queries2.sql`. Ne peut pas être utilisé simultanément avec `--query`.
* `--multiquery, -n` – Si cette option est spécifiée, plusieurs requêtes séparées par des points-virgules peuvent être indiquées après l&#39;option `--query`. Pour plus de commodité, il est également possible d&#39;omettre `--query` et de passer les requêtes directement après `--multiquery`.
* `-N`, `--table` — nom de la table dans laquelle placer les données de sortie, `table` par défaut.
* `-f`, `--format`, `--output-format` — format de sortie, `TSV` par défaut.
* `-d`, `--database` — base de données par défaut, `_local` par défaut.
* `--stacktrace` — indique s&#39;il faut produire une sortie de débogage en cas d&#39;exception.
* `--echo [ <bool> ]` — affiche chaque requête avant son exécution. Accepte une valeur booléenne facultative. Activé par défaut en mode interactif et désactivé en mode batch. Remarque : comme `--echo` accepte désormais une valeur facultative, une requête positionnelle placée immédiatement après un `--echo` sans valeur est interprétée comme sa valeur ; utilisez plutôt `--echo --query "..."`, `--echo -q "..."`, `--echo=false` ou `stdin` via un pipe.
* `--echo-formatted [ <bool> ]` — formate les requêtes affichées en écho. Accepte une valeur booléenne facultative. Activé par défaut en mode interactif et désactivé en mode batch.
* `--echo-query-id [ <bool> ]` — affiche le `query_id` avant l&#39;exécution. Accepte une valeur booléenne facultative. Activé par défaut en mode interactif et désactivé en mode batch.
* `--echo-query-separator <string>` — affiche ce séparateur avant la requête formatée affichée en écho (nécessite `--echo-formatted`), afin de mieux distinguer la requête saisie de son écho reformaté. Vide par défaut (désactivé).
* `--highlight`, `--hilite` `<bool>` — active ou désactive la coloration syntaxique de l&#39;invite de commande et des requêtes affichées en écho. Activée par défaut. La coloration n&#39;est appliquée que lors de l&#39;écriture dans un terminal.
* `--hints <bool>` — affiche des indication d&#39;autocomplétion à la saisie (texte &quot;fantôme&quot; intégré) pour la meilleure suggestion correspondante lorsque le curseur se trouve à la fin de l&#39;entrée. Parcourez les indication avec Haut/Bas (ou Ctrl-Haut/Ctrl-Bas) ; acceptez l&#39;indication intégrée avec Tab ou Droite ; `Enter` n&#39;accepte une indication qu&#39;après qu&#39;elle a été explicitement sélectionnée, sinon il exécute la requête ; `Tab` ouvre également la liste classique de complétion. Nécessite `--highlight` (les indication ont besoin de couleur) et le mécanisme de suggestion (donc `--disable_suggestion` les désactive également). Activé par défaut.
* `--verbose` — affiche plus de détails sur l&#39;exécution des requêtes.
* `--logger.console` — écrire les logs dans la console.
* `--logger.log` — nom du fichier journal.
* `--logger.level` — niveau de journalisation.
* `--ignore-error` — ne pas arrêter le traitement si une requête a échoué.
* `-c`, `--config-file` — chemin du fichier de configuration, dans le même format que pour ClickHouse server ; par défaut, la configuration est vide.
* `--no-system-tables` — ne pas attacher les tables système.
* `--help` — référence des arguments pour `clickhouse-local`.
* `-V`, `--version` — affiche les informations de version et quitte.

Il existe également des arguments pour chaque variable de configuration de ClickHouse, plus couramment utilisés à la place de `--config-file`.

<div id="commands">
  ## Commandes
</div>

<div id="ls-command">
  ### Commande LS
</div>

Répertorie tous les fichiers du répertoire de travail courant accessibles à clickhouse-local.

Vous pouvez l’exécuter en mode interactif comme ceci :

```sql title="Query"
ClickHouse local version 26.3.1.1.

:) ls

SELECT _file AS file
FROM file('*', 'One')
ORDER BY file ASC
```

```text title="Response"
┌─file────────┐
│ file1.csv   │
│ file2.json  │
│ file3.xml   │
└─────────────┘
```

Vous pouvez également l’exécuter sous forme de requête à l’aide de l’argument `-q` :

```sh
./clickhouse-local -q ls
```

```text title="Response"
file1.csv
file2.json
file3.xml
```

<div id="clear-command">
  ### Commande CLEAR
</div>

Efface l’écran du terminal (comme la commande `clear` sous Linux ou Ctrl+L dans de nombreux terminaux). Il s’agit d’une action côté client : elle n’est pas envoyée au moteur SQL.

Dans `clickhouse-local`, la méta-commande est reconnue en mode **interactif** ainsi qu’avec les entrées **`-q`** et **`--queries-file`** (même chemin client que `-q`, même principe que `ls`), de sorte qu’un simple `clear` ne produit pas d’erreur `UNKNOWN_IDENTIFIER`. Avec **`clickhouse-client --queries-file`** à distance, rien ne change : le contenu du fichier est exécuté uniquement comme du SQL (pas de méta-commandes au niveau du texte).

Dans `clickhouse-client`, elle est reconnue uniquement en mode **interactif**. Avec **`-q`** ou des fichiers de requête, `clear` est toujours interprété comme du SQL, de sorte que l’automatisation conserve le comportement d’erreur précédent au lieu de transformer des fautes de frappe en no-op silencieux.

Formes prises en charge : `clear`, `CLEAR`, `/clear` (un `;` final facultatif est ignoré). Si la sortie standard n’est pas un terminal (par exemple, lorsque la sortie passe par un pipe), la méta-commande est acceptée lorsqu’elle est reconnue, mais n’émet pas de séquences de contrôle.

Avec `clickhouse-local` et `-q` :

```sh
./clickhouse-local -q clear
```

<div id="examples">
  ## Exemples
</div>

```bash title="Query"
$ echo -e "1,2\n3,4" | clickhouse-local --structure "a Int64, b Int64" \
    --input-format "CSV" --query "SELECT * FROM table"
Read 2 rows, 32.00 B in 0.000 sec., 5182 rows/sec., 80.97 KiB/sec.
1   2
3   4
```

L’exemple précédent est identique à :

```bash title="Query"
$ echo -e "1,2\n3,4" | clickhouse-local -n --query "
    CREATE TABLE table (a Int64, b Int64) ENGINE = File(CSV, stdin);
    SELECT a, b FROM table;
    DROP TABLE table;"
Read 2 rows, 32.00 B in 0.000 sec., 4987 rows/sec., 77.93 KiB/sec.
1   2
3   4
```

Vous n&#39;avez pas besoin d&#39;utiliser `stdin` ni l&#39;argument `--file`, et vous pouvez ouvrir autant de fichiers que vous le souhaitez à l&#39;aide de la [fonction de table `file`](../../sql-reference/table-functions/file.md) :

```bash title="Query"
$ echo 1 | tee 1.tsv
1

$ echo 2 | tee 2.tsv
2

$ clickhouse-local --query "
    select * from file('1.tsv', TSV, 'a int') t1
    cross join file('2.tsv', TSV, 'b int') t2"
1    2
```

Affichons maintenant l’utilisateur memory pour chaque utilisateur Unix :

```bash title="Query"
$ ps aux | tail -n +2 | awk '{ printf("%s\t%s\n", $1, $4) }' \
    | clickhouse-local --structure "user String, mem Float64" \
        --query "SELECT user, round(sum(mem), 2) as memTotal
            FROM table GROUP BY user ORDER BY memTotal DESC FORMAT Pretty"
```

```text title="Response"
Read 186 rows, 4.15 KiB in 0.035 sec., 5302 rows/sec., 118.34 KiB/sec.
┏━━━━━━━━━━┳━━━━━━━━━━┓
┃ user     ┃ memTotal ┃
┡━━━━━━━━━━╇━━━━━━━━━━┩
│ bayonet  │    113.5 │
├──────────┼──────────┤
│ root     │      8.8 │
├──────────┼──────────┤
...
```

<div id="starting-listeners">
  ## Démarrage des points d’écoute TCP et HTTP
</div>

`clickhouse-local` peut être transformé en serveur léger acceptant les connexions TCP (protocole natif) et HTTP. Cela est utile lorsque vous souhaitez permettre à d’autres outils ou applications ClickHouse d’accéder aux bases de données et aux tables d’une instance `clickhouse-local` en cours de fonctionnement. Notez que chaque connexion entrante dispose de sa propre session : les tables temporaires et les paramètres de session de la session interactive `clickhouse-local` ne sont pas visibles depuis les connexions externes.

Utilisez `SYSTEM START LISTEN` pour ouvrir un point d’écoute et `SYSTEM STOP LISTEN` pour le fermer :

```bash
clickhouse-local \
    --listen_host 127.0.0.1 \
    --tcp_port 9000 \
    --http_port 8123 \
    --query "
        SYSTEM START LISTEN TCP;
        SYSTEM START LISTEN HTTP;
        SELECT * FROM url('http://127.0.0.1:8123/?query=SELECT+42', LineAsString);
        SYSTEM STOP LISTEN TCP;
        SYSTEM STOP LISTEN HTTP;
    "
```

Les options `--listen_host`, `--tcp_port` et `--http_port` configurent l’adresse d’écoute et les ports. Les ports par défaut sont `9000` pour TCP et `8123` pour HTTP.

:::warning Sécurité
Par défaut, `clickhouse-local` s’exécute avec une configuration d’utilisateurs temporaires ; tout point d’écoute qu’il ouvre n’est donc pas authentifié. Utilisez une adresse de bouclage (`127.0.0.1` ou `::1`), sauf si vous avez explicitement configuré des utilisateurs et le contrôle d’accès en faisant pointer le paramètre `users_config` vers un fichier `users.xml` personnalisé (par exemple via `--config-file`). Écouter sur une adresse autre que de bouclage sans authentification expose les données de l’instance locale à toute personne pouvant atteindre le port choisi.
:::

<div id="related-content-1">
  ## Contenu connexe
</div>

* [Extraire, convertir et interroger des données dans des fichiers locaux avec clickhouse-local](https://clickhouse.com/blog/extracting-converting-querying-local-files-with-sql-clickhouse-local)
* [Importer des données dans ClickHouse - Partie 1](https://clickhouse.com/blog/getting-data-into-clickhouse-part-1)
* [Explorer des jeux de données massifs du monde réel : plus de 100 ans de relevés météorologiques dans ClickHouse](https://clickhouse.com/blog/real-world-data-noaa-climate-data)
* Blog : [Extraire, convertir et interroger des données dans des fichiers locaux avec clickhouse-local](https://clickhouse.com/blog/extracting-converting-querying-local-files-with-sql-clickhouse-local)