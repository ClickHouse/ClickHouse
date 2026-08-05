---
description: 'Documentation d’Odbc Bridge'
slug: /operations/utilities/odbc-bridge
title: 'clickhouse-odbc-bridge'
doc_type: 'reference'
---

Un simple serveur HTTP faisant office de proxy pour le pilote ODBC. La principale motivation
était les possibles segfaults ou autres défaillances dans les implémentations ODBC, qui peuvent
faire planter l’ensemble du processus clickhouse-server.

Cet outil fonctionne via HTTP, et non via des pipes, de la mémoire partagée ou TCP, car :

* Il est plus simple à implémenter
* Il est plus simple à déboguer
* jdbc-bridge peut être implémenté de la même manière

<div id="usage">
  ## Utilisation
</div>

`clickhouse-server` utilise cet outil dans la table function odbc et StorageODBC.
Cependant, il peut aussi être utilisé comme outil autonome en ligne de commande, avec les
paramètres suivants dans l’URL d’une requête POST :

* `connection_string` -- chaîne de connexion ODBC.
* `sample_block` -- description des colonnes au format ClickHouse NamesAndTypesList, avec le nom entre backticks
  et le type sous forme de chaîne. Le nom et le type sont séparés par un espace, et les lignes par
  des sauts de ligne.
* `max_block_size` -- paramètre facultatif qui définit la taille maximale d’un seul bloc.
  La requête est envoyée dans le corps de la requête POST. La réponse est renvoyée au format RowBinary.

<div id="example">
  ## Exemple :
</div>

```bash
$ clickhouse-odbc-bridge --http-port 9018 --daemon

$ curl -d "query=SELECT PageID, ImpID, AdType FROM Keys ORDER BY PageID, ImpID" --data-urlencode "connection_string=DSN=ClickHouse;DATABASE=stat" --data-urlencode "sample_block=columns format version: 1
3 columns:
\`PageID\` String
\`ImpID\` String
\`AdType\` String
"  "http://localhost:9018/" > result.txt

$ cat result.txt
12246623837185725195925621517
```