---
description: "ClickHouse permet d'envoyer à un serveur les données nécessaires au traitement
  d'une requête, avec une requête `SELECT`. Ces données sont placées dans une table temporaire et
  peuvent être utilisées dans la requête (par exemple, dans les opérateurs `IN`)."
sidebar_label: 'Données externes pour le traitement des requêtes'
sidebar_position: 130
slug: /engines/table-engines/special/external-data
title: 'Données externes pour le traitement des requêtes'
doc_type: 'reference'
---

ClickHouse permet d&#39;envoyer à un serveur les données nécessaires au traitement d&#39;une requête, avec une requête `SELECT`. Ces données sont placées dans une table temporaire (voir la section &quot;Tables temporaires&quot;) et peuvent être utilisées dans la requête (par exemple, dans les opérateurs `IN`).

Par exemple, si vous avez un fichier texte contenant des identifiants utilisateur importants, vous pouvez l&#39;envoyer au serveur avec une requête qui filtre selon cette liste.

Si vous devez exécuter plusieurs requêtes avec un volume important de données externes, n&#39;utilisez pas cette fonctionnalité. Il est préférable d&#39;importer les données dans la base de données à l&#39;avance.

Les données externes peuvent être envoyées à l&#39;aide du client en ligne de commande (en mode non interactif) ou via l&#39;interface HTTP.

Dans le client en ligne de commande, vous pouvez spécifier une section de paramètres au format

```bash
--external --file=... [--name=...] [--format=...] [--types=...|--structure=...]
```

Vous pouvez avoir plusieurs sections de ce type, selon le nombre de tables transmises.

**–external** – Indique le début d&#39;une clause.
**–file** – Chemin vers le fichier contenant le dump de la table, ou -, qui fait référence à stdin.
Une seule table peut être lue depuis stdin.

Les paramètres suivants sont facultatifs : **–name**– Nom de la table. S&#39;il est omis, &#95;data est utilisé.
**–format** – Format des données dans le fichier. S&#39;il est omis, TabSeparated est utilisé.

L&#39;un des paramètres suivants est obligatoire :**–types** – Une liste de types de colonnes séparés par des virgules. Par exemple : `UInt64,String`. Les colonnes seront nommées &#95;1, &#95;2, ...
**–structure**– La structure de la table au format`UserID UInt64`, `URL String`. Définit les noms et les types des colonnes.

Les fichiers spécifiés dans &#39;file&#39; seront analysés selon le format spécifié dans &#39;format&#39;, en utilisant les types de données indiqués dans &#39;types&#39; ou &#39;structure&#39;. La table sera envoyée au serveur et y sera accessible en tant que table temporaire sous le nom indiqué dans &#39;name&#39;.

Exemples :

```bash
$ echo -ne "1\n2\n3\n" | clickhouse-client --query="SELECT count() FROM test.visits WHERE TraficSourceID IN _data" --external --file=- --types=Int8
849897
$ cat /etc/passwd | sed 's/:/\t/g' | clickhouse-client --query="SELECT shell, count() AS c FROM passwd GROUP BY shell ORDER BY c DESC" --external --file=- --name=passwd --structure='login String, unused String, uid UInt16, gid UInt16, comment String, home String, shell String'
/bin/sh 20
/bin/false      5
/bin/bash       4
/usr/sbin/nologin       1
/bin/sync       1
```

Lors de l’utilisation de l’interface HTTP, les données externes sont transmises au format multipart/form-data. Chaque table est transmise sous la forme d’un fichier distinct. Le nom de la table est déduit du nom du fichier. Les paramètres `name_format`, `name_types` et `name_structure` sont transmis via `query_string`, où `name` correspond au nom de la table à laquelle ces paramètres s’appliquent. La signification de ces paramètres est la même que lors de l’utilisation du client en ligne de commande.

Exemple :

```bash
$ cat /etc/passwd | sed 's/:/\t/g' > passwd.tsv

$ curl -F 'passwd=@passwd.tsv;' 'http://localhost:8123/?query=SELECT+shell,+count()+AS+c+FROM+passwd+GROUP+BY+shell+ORDER+BY+c+DESC&passwd_structure=login+String,+unused+String,+uid+UInt16,+gid+UInt16,+comment+String,+home+String,+shell+String'
/bin/sh 20
/bin/false      5
/bin/bash       4
/usr/sbin/nologin       1
/bin/sync       1
```

Dans le cadre du traitement distribué des requêtes, les tables temporaires sont envoyées à tous les serveurs distants.