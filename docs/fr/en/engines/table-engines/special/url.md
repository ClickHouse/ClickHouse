---
description: 'Interroge des données sur un serveur HTTP/HTTPS distant. Ce moteur est similaire
  au moteur File.'
sidebar_label: 'URL'
sidebar_position: 80
slug: /engines/table-engines/special/url
title: 'Moteur de table URL'
doc_type: 'référence'
---

Interroge des données sur un serveur HTTP/HTTPS distant. Ce moteur est similaire au moteur [File](../../../engines/table-engines/special/file.md).

Syntaxe : `URL(URL [,Format] [,CompressionMethod])`

* Le paramètre `URL` doit respecter la structure d&#39;un Uniform Resource Locator. Pour une `URL` `http`/`https` (le backend par défaut), il doit pointer vers un serveur qui utilise HTTP ou HTTPS, et l&#39;obtention d&#39;une réponse du serveur ne doit nécessiter aucun en-tête supplémentaire. Une URL avec un schéma non HTTP reconnu (`file://`, `s3://`, `az://`, `hdfs://`, …) est au contraire déléguée au moteur correspondant — voir [routage selon le schéma d’URL](#scheme-dispatch) ci-dessous.

* Le `Format` doit être un format que ClickHouse peut utiliser dans les requêtes `SELECT` et, si nécessaire, dans les requêtes `INSERT`. Pour la liste complète des formats pris en charge, voir [Formats](/fr/interfaces/formats#formats-overview).

  Si cet argument n&#39;est pas spécifié, ClickHouse détecte automatiquement le format à partir du suffixe du paramètre `URL`. Si le suffixe du paramètre `URL` ne correspond à aucun format pris en charge, la création de la table échoue. Par exemple, pour l&#39;expression de moteur `URL('http://localhost/test.json')`, le format `JSON` est appliqué.

* `CompressionMethod` indique si le corps HTTP doit être compressé. Si la compression est activée, les paquets HTTP envoyés par le moteur URL contiennent l&#39;en-tête &#39;Content-Encoding&#39; pour indiquer la méthode de compression utilisée.

Pour activer la compression, assurez-vous d&#39;abord que le point de terminaison HTTP distant indiqué par le paramètre `URL` prend en charge l&#39;algorithme de compression correspondant.

Le `CompressionMethod` pris en charge doit être l&#39;un des suivants :

* gzip ou gz
* deflate
* brotli ou br
* lzma ou xz
* zstd ou zst
* lz4
* bz2
* snappy
* none
* auto

Si `CompressionMethod` n&#39;est pas spécifié, la valeur par défaut est `auto`. Cela signifie que ClickHouse détecte automatiquement la méthode de compression à partir du suffixe du paramètre `URL`. Si le suffixe correspond à l&#39;une des méthodes de compression listées ci-dessus, la compression correspondante est appliquée ; sinon, aucune compression n&#39;est activée.

Par exemple, pour l&#39;expression de moteur `URL('http://localhost/test.gzip')`, la méthode de compression `gzip` est appliquée, mais pour `URL('http://localhost/test.fr')`, aucune compression n&#39;est activée, car le suffixe `fr` ne correspond à aucune des méthodes de compression ci-dessus.

<div id="scheme-dispatch">
  ## Routage selon le schéma d’URL
</div>

Le moteur `URL` est un wrapper unifié au-dessus des autres moteurs de fichiers et de stockage objet : il route vers le backend approprié en fonction du schéma d’URL. `http`/`https` (ainsi que tout schéma non reconnu) sont pris en charge par le moteur `URL` lui-même ; `file://` est pris en charge par le moteur [File](../../../engines/table-engines/special/file.md) ; `s3://`, `gs://`, `gcs://`, `oss://` par le moteur [S3](/fr/engines/table-engines/integrations/s3) ; `az://`, `azure://`, `abfss://`, `abfs://` par le moteur [AzureBlobStorage](/fr/engines/table-engines/integrations/azureBlobStorage) ; et `hdfs://` par le moteur [HDFS](/fr/engines/table-engines/integrations/hdfs).

Seuls les schémas S3 que le mappeur d’URI S3 résout vers un endpoint concret sans configuration supplémentaire (`s3`, ainsi que `gs`/`gcs`/`oss`) sont routés. Les autres schémas de fournisseurs compatibles S3 (`cos`, `obs`, `eos`, …) sont spécifiques à une région et ne disposent d’aucune correspondance d’endpoint par défaut ; ainsi, transmettre une telle URL au moteur `URL` la fait traiter comme un schéma non reconnu et la signale comme une erreur. Pour ces backends, utilisez directement le moteur [S3](/fr/engines/table-engines/integrations/s3) (avec `url_scheme_mappers` configuré).

Le paramètre [url&#95;base](/fr/operations/settings/settings.md#url_base) est appliqué avant le routage par schéma, de sorte qu’une référence relative est d’abord résolue par rapport à la base, puis acheminée vers le moteur correspondant.

```sql
CREATE TABLE file_via_url (a UInt32, b String) ENGINE = URL('file://data.csv', CSV);
CREATE TABLE s3_via_url (a UInt32, b String) ENGINE = URL('s3://bucket/key.csv', CSV);
```

<div id="using-the-engine-in-the-clickhouse-server">
  ## Utilisation
</div>

Les requêtes `INSERT` et `SELECT` sont converties en requêtes `POST` et `GET`,
respectivement. Pour le traitement des requêtes `POST`, le serveur distant doit prendre en charge
l’[encodage de transfert par blocs](https://en.wikipedia.org/wiki/Chunked_transfer_encoding).

Vous pouvez limiter le nombre maximal de redirections HTTP GET à l’aide du paramètre [max&#95;http&#95;get&#95;redirects](/fr/operations/settings/settings#max_http_get_redirects).

<div id="wildcards-with-http-index-pages">
  ## Caractères génériques avec les pages d’index HTTP
</div>

Lorsque [allow&#95;experimental&#95;url&#95;wildcard&#95;from&#95;index&#95;pages](/fr/operations/settings/settings.md#allow_experimental_url_wildcard_from_index_pages) est activé, le moteur de table `URL` peut étendre les caractères génériques en récupérant des pages d’index HTTP et en en extrayant les liens.
Il s’agit du même mécanisme que pour la fonction de table [`url`](../../../sql-reference/table-functions/url.md#wildcards-with-http-index-pages).

L’expansion est limitée par [max&#95;http&#95;index&#95;page&#95;size](/fr/operations/server-configuration-parameters/settings.md#max_http_index_page_size) pour chaque page d’index récupérée, et par [url&#95;wildcard&#95;max&#95;directories&#95;to&#95;read](/fr/operations/settings/settings.md#url_wildcard_max_directories_to_read) pour le parcours récursif des répertoires.

<div id="example">
  ## Exemple
</div>

**1.** Créez une table `url_engine_table` sur le serveur :

```sql
CREATE TABLE url_engine_table (word String, value UInt64)
ENGINE=URL('http://127.0.0.1:12345/', CSV)
```

**2.** Créez un serveur HTTP simple à l’aide des outils standard de Python 3 et
démarrez-le :

```python3
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

```bash
$ python3 server.py
```

**3.** Demander les données :

```sql
SELECT * FROM url_engine_table
```

```text
┌─word──┬─value─┐
│ Hello │     1 │
│ World │     2 │
└───────┴───────┘
```

<div id="details-of-implementation">
  ## Détails de l’implémentation
</div>

* Les lectures et les écritures peuvent être effectuées en parallèle
* Non pris en charge :
  * Les opérations `ALTER` et `SELECT...SAMPLE`.
  * Les index.
  * La réplication.

<div id="virtual-columns">
  ## Colonnes virtuelles
</div>

* `_path` — Chemin de l’`URL`. Type : `LowCardinality(String)`.
* `_file` — Nom de la ressource dans l’`URL`. Type : `LowCardinality(String)`.
* `_size` — Taille de la ressource en octets. Type : `Nullable(UInt64)`. Si la taille est inconnue, la valeur est `NULL`.
* `_time` — Date et heure de dernière modification du fichier. Type : `Nullable(DateTime)`. Si cette information est inconnue, la valeur est `NULL`.
* `_headers` - En-têtes de la réponse HTTP. Type : `Map(LowCardinality(String), LowCardinality(String))`.

<div id="resolving-relative-urls">
  ## Résolution des URL relatives
</div>

Le paramètre [url&#95;base](/fr/operations/settings/settings.md#url_base) permet d’utiliser une URL relative dans le moteur `URL`. Lorsque `url_base` est défini, l’URL transmise au moteur est résolue par rapport à cette base conformément à la [RFC 3986](https://datatracker.ietf.org/doc/html/rfc3986). Pour une description complète des règles de résolution, consultez la [documentation de la fonction de table url](../../../sql-reference/table-functions/url.md#resolving-relative-urls).

**Exemple**

```sql
SET url_base = 'http://127.0.0.1:12345/';
CREATE TABLE url_engine_table (word String, value UInt64) ENGINE = URL('hello.csv', CSV);
SELECT * FROM url_engine_table;
```

<div id="storage-settings">
  ## Paramètres de stockage
</div>

* [engine&#95;url&#95;skip&#95;empty&#95;files](/fr/operations/settings/settings.md#engine_url_skip_empty_files) - permet d’ignorer les fichiers vides lors de la lecture. Désactivé par défaut.
* [enable&#95;url&#95;encoding](/fr/operations/settings/settings.md#enable_url_encoding) - permet d’activer ou de désactiver le décodage/l’encodage du chemin dans l’URI. Activé par défaut.
* [url&#95;base](/fr/operations/settings/settings.md#url_base) - URL de base pour résoudre les URL relatives transmises au moteur.