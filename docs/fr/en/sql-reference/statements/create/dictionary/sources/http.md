---
slug: /sql-reference/statements/create/dictionary/sources/http
title: 'Source de dictionnaire HTTP(S)'
sidebar_position: 5
sidebar_label: 'HTTP(S)'
description: 'Configurez un endpoint HTTP ou HTTPS comme source de dictionnaire dans ClickHouse.'
doc_type: 'référence'
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

Le fonctionnement avec un serveur HTTP(S) dépend de [la manière dont le dictionnaire est stocké en mémoire](../layouts/). Si le dictionnaire est stocké à l’aide de `cache` et `complex_key_cache`, ClickHouse demande les clés nécessaires en envoyant une requête via la méthode `POST`.

Exemple de paramètres :

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    SOURCE(HTTP(
        url 'http://[::1]/os.tsv'
        format 'TabSeparated'
        credentials(user 'user' password 'password')
        headers(header(name 'API-KEY' value 'key'))
    ))
    ```
  </TabItem>

  <TabItem value="xml" label="Fichier de configuration">
    ```xml
    <source>
        <http>
            <url>http://[::1]/os.tsv</url>
            <format>TabSeparated</format>
            <credentials>
                <user>user</user>
                <password>password</password>
            </credentials>
            <headers>
                <header>
                    <name>API-KEY</name>
                    <value>key</value>
                </header>
            </headers>
        </http>
    </source>
    ```
  </TabItem>
</Tabs>

<br />

Pour que ClickHouse puisse accéder à une ressource HTTPS, vous devez [configurer OpenSSL](/fr/operations/server-configuration-parameters/settings#openssl) dans la configuration du serveur.

Champs des paramètres :

| Paramètre     | Description                                                                                                |
| ------------- | ---------------------------------------------------------------------------------------------------------- |
| `url`         | L’URL source.                                                                                              |
| `format`      | Le format du fichier. Tous les formats décrits dans [Formats](/fr/sql-reference/formats) sont pris en charge. |
| `credentials` | Authentification HTTP Basic. Facultatif.                                                                   |
| `user`        | Nom d’utilisateur requis pour l’authentification.                                                          |
| `password`    | Mot de passe requis pour l’authentification.                                                               |
| `headers`     | Ensemble des entrées d’en-têtes HTTP personnalisés utilisées pour la requête HTTP. Facultatif.             |
| `header`      | Entrée d’en-tête HTTP unique.                                                                              |
| `name`        | Nom de l’identifiant utilisé pour l’en-tête envoyé dans la requête.                                        |
| `value`       | Valeur définie pour un nom d’identifiant donné.                                                            |

Lors de la création d’un dictionnaire à l’aide de la commande DDL (`CREATE DICTIONARY ...`), les hôtes distants des dictionnaires HTTP sont vérifiés par rapport au contenu de la section `remote_url_allow_hosts` du fichier de configuration afin d’empêcher les utilisateurs de base de données d’accéder à un serveur HTTP arbitraire.