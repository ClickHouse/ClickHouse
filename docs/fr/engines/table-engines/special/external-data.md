---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 34
toc_title: "De donn\xE9es externes"
---

# Données externes pour le traitement des requêtes {#external-data-for-query-processing}

ClickHouse permet d'Envoyer à un serveur les données nécessaires au traitement d'une requête, ainsi qu'une requête SELECT. Ces données sont placées dans une table temporaire (voir la section “Temporary tables”) et peut être utilisé dans la requête (par exemple, dans les DANS les opérateurs).

Par exemple, si vous disposez d'un fichier texte avec important des identifiants d'utilisateur, vous pouvez le télécharger sur le serveur avec une requête qui utilise la filtration par cette liste.

Si vous devez exécuter plusieurs requêtes avec un volume important de données externes, n'utilisez pas cette fonctionnalité. Il est préférable de télécharger les données sur la base de données à l'avance.

Les données externes peuvent être téléchargées à l'aide du client de ligne de commande (en mode non interactif) ou à l'aide de L'interface HTTP.

Dans le client de ligne de commande, vous pouvez spécifier une section paramètres du format

``` bash
--external --file=... [--name=...] [--format=...] [--types=...|--structure=...]
```

Vous pouvez avoir plusieurs sections comme ça, pour le nombre de tables étant transmis.

**–external** – Marks the beginning of a clause.
**–file** – Path to the file with the table dump, or -, which refers to stdin.
Une seule table peut être récupérée à partir de stdin.

Les paramètres suivants sont facultatifs: **–name**– Name of the table. If omitted, _data is used.
**–format** – Data format in the file. If omitted, TabSeparated is used.

L'un des paramètres suivants est requis:**–types** – A list of comma-separated column types. For example: `UInt64,String`. The columns will be named _1, _2, …
**–structure**– The table structure in the format`UserID UInt64`, `URL String`. Définit les noms et les types de colonnes.

Les fichiers spécifiés dans ‘file’ sera analysé par le format spécifié dans ‘format’, en utilisant les types de données spécifié dans ‘types’ ou ‘structure’. La table sera téléchargée sur le serveur et accessible en tant que table temporaire avec le nom dans ‘name’.

Exemple:

``` bash
$ echo -ne "1\n2\n3\n" | clickhouse-client --query="SELECT count() FROM test.visits WHERE TraficSourceID IN _data" --external --file=- --types=Int8
849897
$ cat /etc/passwd | sed 's/:/\t/g' | clickhouse-client --query="SELECT shell, count() AS c FROM passwd GROUP BY shell ORDER BY c DESC" --external --file=- --name=passwd --structure='login String, unused String, uid UInt16, gid UInt16, comment String, home String, shell String'
/bin/sh 20
/bin/false      5
/bin/bash       4
/usr/sbin/nologin       1
/bin/sync       1
```

Lors de l'utilisation de L'interface HTTP, les données externes sont transmises au format multipart/form-data. Chaque tableau est transmis en tant que fichier séparé. Le nom de la table est tiré du nom du fichier. Le ‘query_string’ est passé les paramètres ‘name_format’, ‘name_types’, et ‘name_structure’, où ‘name’ est le nom de la table que ces paramètres correspondent. La signification des paramètres est la même que lors de l'utilisation du client de ligne de commande.

Exemple:

``` bash
$ cat /etc/passwd | sed 's/:/\t/g' > passwd.tsv

$ curl -F 'passwd=@passwd.tsv;' 'http://localhost:8123/?query=SELECT+shell,+count()+AS+c+FROM+passwd+GROUP+BY+shell+ORDER+BY+c+DESC&passwd_structure=login+String,+unused+String,+uid+UInt16,+gid+UInt16,+comment+String,+home+String,+shell+String'
/bin/sh 20
/bin/false      5
/bin/bash       4
/usr/sbin/nologin       1
/bin/sync       1
```

Pour le traitement des requêtes distribuées, les tables temporaires sont envoyées à tous les serveurs distants.

[Article Original](https://clickhouse.tech/docs/en/operations/table_engines/external_data/) <!--hide-->
