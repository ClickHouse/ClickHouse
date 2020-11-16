---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 41
toc_title: URL
---

# URL (URL, Format) {#table_engines-url}

Gère les données sur un serveur HTTP / HTTPS distant. Ce moteur est similaire
à l' [Fichier](file.md) moteur.

## Utilisation du moteur dans le serveur ClickHouse {#using-the-engine-in-the-clickhouse-server}

Le `format` doit être celui que ClickHouse peut utiliser dans
`SELECT` les requêtes et, si nécessaire, en `INSERTs`. Pour la liste complète des formats pris en charge, voir
[Format](../../../interfaces/formats.md#formats).

Le `URL` doit être conforme à la structure D'un Localisateur de ressources uniforme. L'URL spécifiée doit pointer vers un serveur
qui utilise le protocole HTTP ou HTTPS. Cela ne nécessite pas de
en-têtes supplémentaires pour obtenir une réponse du serveur.

`INSERT` et `SELECT` les requêtes sont transformées en `POST` et `GET` demande,
respectivement. Pour le traitement `POST` demandes, le serveur distant doit prendre en charge
[Encodage de transfert en morceaux](https://en.wikipedia.org/wiki/Chunked_transfer_encoding).

Vous pouvez limiter le nombre maximal de sauts de redirection HTTP GET en utilisant [max_http_get_redirects](../../../operations/settings/settings.md#setting-max_http_get_redirects) paramètre.

**Exemple:**

**1.** Créer un `url_engine_table` table sur le serveur :

``` sql
CREATE TABLE url_engine_table (word String, value UInt64)
ENGINE=URL('http://127.0.0.1:12345/', CSV)
```

**2.** Créez un serveur HTTP de base à l'aide des outils Python 3 standard et
démarrer:

``` python3
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

``` bash
$ python3 server.py
```

**3.** Les données de la demande:

``` sql
SELECT * FROM url_engine_table
```

``` text
┌─word──┬─value─┐
│ Hello │     1 │
│ World │     2 │
└───────┴───────┘
```

## Les détails de mise en Œuvre {#details-of-implementation}

-   Les lectures et les écritures peuvent être parallèles
-   Pas pris en charge:
    -   `ALTER` et `SELECT...SAMPLE` opérations.
    -   Index.
    -   Réplication.

[Article Original](https://clickhouse.tech/docs/en/operations/table_engines/url/) <!--hide-->
