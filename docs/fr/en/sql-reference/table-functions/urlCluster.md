---
description: 'Permet de traiter en parallèle, depuis plusieurs nœuds d''un cluster spécifié, des fichiers accessibles par URL.'
sidebar_label: 'urlCluster'
sidebar_position: 201
slug: /sql-reference/table-functions/urlCluster
title: 'urlCluster'
doc_type: 'reference'
---

Permet de traiter en parallèle, depuis plusieurs nœuds d&#39;un cluster spécifié, des fichiers accessibles par URL. Sur l&#39;initiateur, elle établit une connexion à tous les nœuds du cluster, développe l&#39;astérisque dans le chemin de fichier de l&#39;URL et répartit dynamiquement chaque fichier. Sur le nœud worker, elle demande à l&#39;initiateur la tâche suivante à traiter, puis l&#39;exécute. Ce processus se répète jusqu&#39;à ce que toutes les tâches soient terminées.

<div id="syntax">
  ## Syntaxe
</div>

```sql
urlCluster(cluster_name, URL, format, structure)
```

<div id="arguments">
  ## Arguments
</div>

| Argument       | Description                                                                                                                                                             |
| -------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `cluster_name` | Nom d’un cluster utilisé pour constituer un ensemble d’adresses et de paramètres de connexion vers des serveurs distants et locaux.                                     |
| `URL`          | Adresse du serveur HTTP ou HTTPS pouvant accepter des requêtes `GET`. Type : [String](../../sql-reference/data-types/string.md).                                        |
| `format`       | [Format](/fr/sql-reference/formats) des données. Type : [String](../../sql-reference/data-types/string.md).                                                                |
| `structure`    | Structure de la table au format `'UserID UInt64, Name String'`. Détermine les noms et les types de colonnes. Type : [String](../../sql-reference/data-types/string.md). |

<div id="returned_value">
  ## Valeur renvoyée
</div>

Une table au format et à la structure spécifiés, contenant des données provenant de l’`URL` définie.

<div id="examples">
  ## Exemples
</div>

Récupération des 3 premières lignes d’une table contenant des colonnes de type `String` et [UInt32](../../sql-reference/data-types/int-uint.md) depuis un serveur HTTP qui répond au format [CSV](/fr/interfaces/formats/CSV).

1. Créez un serveur HTTP basique à l’aide des outils standard de Python 3 et démarrez-le :

```python
from http.server import BaseHTTPRequestHandler, HTTPServer

class CSVHTTPServer(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header('Content-type', 'text/csv')
        self.end_headers()

        self.wfile.write(bytes('Hello,1\nWorld,2\n', "utf-8"))

if __name__ == "__main__":
    server_address = ('127.0.0.1', 12345)
    HTTPServer(server_address, CSVHTTPServer).serve_forever()
```

```sql
SELECT * FROM urlCluster('cluster_simple','http://127.0.0.1:12345', CSV, 'column1 String, column2 UInt32')
```

<div id="globs-in-url">
  ## Globs dans l’URL
</div>

Les motifs dans `{ }` sont utilisés pour générer un ensemble de shards ou pour spécifier des adresses de basculement. Pour connaître les types de motifs pris en charge et voir des exemples, consultez la description de la fonction [remote](remote.md#globs-in-addresses).
Le caractère `|` à l’intérieur des motifs sert à spécifier des adresses de basculement. Elles sont parcourues dans le même ordre que celui dans lequel elles apparaissent dans le motif. Le nombre d’adresses générées est limité par le paramètre [glob&#95;expansion&#95;max&#95;elements](../../operations/settings/settings.md#glob_expansion_max_elements).

<div id="related">
  ## Voir aussi
</div>

* [Moteur HDFS](/fr/engines/table-engines/integrations/hdfs)
* [Fonction de table URL](/fr/engines/table-engines/special/url)