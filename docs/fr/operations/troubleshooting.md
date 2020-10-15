---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 46
toc_title: "D\xE9pannage"
---

# Dépannage {#troubleshooting}

-   [Installation](#troubleshooting-installation-errors)
-   [Connexion au serveur](#troubleshooting-accepts-no-connections)
-   [Traitement des requêtes](#troubleshooting-does-not-process-queries)
-   [Efficacité du traitement des requêtes](#troubleshooting-too-slow)

## Installation {#troubleshooting-installation-errors}

### Vous ne pouvez pas obtenir de paquets deb à partir du référentiel ClickHouse avec Apt-get {#you-cannot-get-deb-packages-from-clickhouse-repository-with-apt-get}

-   Vérifiez les paramètres du pare-feu.
-   Si vous ne pouvez pas accéder au référentiel pour quelque raison que ce soit, téléchargez les packages comme décrit dans [Prise en main](../getting-started/index.md) article et les installer manuellement en utilisant le `sudo dpkg -i <packages>` commande. Vous aurez aussi besoin d' `tzdata` paquet.

## Connexion au Serveur {#troubleshooting-accepts-no-connections}

Problèmes possibles:

-   Le serveur n'est pas en cours d'exécution.
-   Paramètres de configuration inattendus ou incorrects.

### Le Serveur N'Est Pas En Cours D'Exécution {#server-is-not-running}

**Vérifiez si le serveur est runnnig**

Commande:

``` bash
$ sudo service clickhouse-server status
```

Si le serveur n'est pas en cours d'exécution, démarrez-le avec la commande:

``` bash
$ sudo service clickhouse-server start
```

**Vérifier les journaux**

Le journal principal de `clickhouse-server` est dans `/var/log/clickhouse-server/clickhouse-server.log` par défaut.

Si le serveur a démarré avec succès, vous devriez voir les chaînes:

-   `<Information> Application: starting up.` — Server started.
-   `<Information> Application: Ready for connections.` — Server is running and ready for connections.

Si `clickhouse-server` démarrage a échoué avec une erreur de configuration, vous devriez voir la `<Error>` chaîne avec une description de l'erreur. Exemple:

``` text
2019.01.11 15:23:25.549505 [ 45 ] {} <Error> ExternalDictionaries: Failed reloading 'event2id' external dictionary: Poco::Exception. Code: 1000, e.code() = 111, e.displayText() = Connection refused, e.what() = Connection refused
```

Si vous ne voyez pas d'erreur à la fin du fichier, parcourez le fichier entier à partir de la chaîne:

``` text
<Information> Application: starting up.
```

Si vous essayez de démarrer une deuxième instance de `clickhouse-server` sur le serveur, vous voyez le journal suivant:

``` text
2019.01.11 15:25:11.151730 [ 1 ] {} <Information> : Starting ClickHouse 19.1.0 with revision 54413
2019.01.11 15:25:11.154578 [ 1 ] {} <Information> Application: starting up
2019.01.11 15:25:11.156361 [ 1 ] {} <Information> StatusFile: Status file ./status already exists - unclean restart. Contents:
PID: 8510
Started at: 2019-01-11 15:24:23
Revision: 54413

2019.01.11 15:25:11.156673 [ 1 ] {} <Error> Application: DB::Exception: Cannot lock file ./status. Another server instance in same directory is already running.
2019.01.11 15:25:11.156682 [ 1 ] {} <Information> Application: shutting down
2019.01.11 15:25:11.156686 [ 1 ] {} <Debug> Application: Uninitializing subsystem: Logging Subsystem
2019.01.11 15:25:11.156716 [ 2 ] {} <Information> BaseDaemon: Stop SignalListener thread
```

**Voir système.d les journaux**

Si vous ne trouvez aucune information utile dans `clickhouse-server` journaux ou il n'y a pas de journaux, vous pouvez afficher `system.d` journaux à l'aide de la commande:

``` bash
$ sudo journalctl -u clickhouse-server
```

**Démarrer clickhouse-server en mode interactif**

``` bash
$ sudo -u clickhouse /usr/bin/clickhouse-server --config-file /etc/clickhouse-server/config.xml
```

Cette commande démarre le serveur en tant qu'application interactive avec les paramètres standard du script de démarrage automatique. Dans ce mode `clickhouse-server` imprime tous les messages d'événement dans la console.

### Paramètres De Configuration {#configuration-parameters}

Vérifier:

-   Le panneau paramètres.

    Si vous exécutez ClickHouse dans Docker dans un réseau IPv6, assurez-vous que `network=host` est définie.

-   Paramètres du point de terminaison.

    Vérifier [listen_host](server-configuration-parameters/settings.md#server_configuration_parameters-listen_host) et [tcp_port](server-configuration-parameters/settings.md#server_configuration_parameters-tcp_port) paramètre.

    Clickhouse server accepte les connexions localhost uniquement par défaut.

-   Paramètres du protocole HTTP.

    Vérifiez les paramètres de protocole pour L'API HTTP.

-   Paramètres de connexion sécurisés.

    Vérifier:

    -   Le [tcp_port_secure](server-configuration-parameters/settings.md#server_configuration_parameters-tcp_port_secure) paramètre.
    -   Paramètres pour [SSL sertificates](server-configuration-parameters/settings.md#server_configuration_parameters-openssl).

    Utilisez les paramètres appropriés lors de la connexion. Par exemple, l'utilisation de la `port_secure` paramètre avec `clickhouse_client`.

-   Les paramètres de l'utilisateur.

    Vous utilisez peut-être un mauvais nom d'utilisateur ou mot de passe.

## Traitement Des Requêtes {#troubleshooting-does-not-process-queries}

Si ClickHouse ne peut pas traiter la requête, il envoie une description d'erreur au client. Dans le `clickhouse-client` vous obtenez une description de l'erreur dans la console. Si vous utilisez L'interface HTTP, ClickHouse envoie la description de l'erreur dans le corps de la réponse. Exemple:

``` bash
$ curl 'http://localhost:8123/' --data-binary "SELECT a"
Code: 47, e.displayText() = DB::Exception: Unknown identifier: a. Note that there are no tables (FROM clause) in your query, context: required_names: 'a' source_tables: table_aliases: private_aliases: column_aliases: public_columns: 'a' masked_columns: array_join_columns: source_columns: , e.what() = DB::Exception
```

Si vous commencez à `clickhouse-client` avec l' `stack-trace` paramètre, ClickHouse renvoie la trace de la pile du serveur avec la description d'une erreur.

Vous pouvez voir un message sur une connexion rompue. Dans ce cas, vous pouvez répéter la requête. Si la connexion se rompt chaque fois que vous effectuez la requête, vérifiez les journaux du serveur pour détecter les erreurs.

## Efficacité du traitement des requêtes {#troubleshooting-too-slow}

Si vous voyez que ClickHouse fonctionne trop lentement, vous devez profiler la charge sur les ressources du serveur et le réseau pour vos requêtes.

Vous pouvez utiliser l'utilitaire clickhouse-benchmark pour profiler les requêtes. Il indique le nombre de requêtes traitées par seconde, le nombre de lignes traitées par seconde, et les percentiles de temps de traitement des requêtes.
