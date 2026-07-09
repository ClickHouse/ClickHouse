---
title: Dépannage
---

[//]: # "Ce fichier est inclus dans la FAQ > Dépannage"

* [Installation](#troubleshooting-installation-errors)
* [Connexion au serveur](#troubleshooting-accepts-no-connections)
* [Traitement des requêtes](#troubleshooting-does-not-process-queries)
* [Performance du traitement des requêtes](#troubleshooting-too-slow)

<div id="troubleshooting-installation-errors">
  ## Installation
</div>

<div id="you-cannot-get-deb-packages-from-clickhouse-repository-with-apt-get">
  ### Vous ne pouvez pas récupérer les paquets deb depuis le dépôt ClickHouse avec apt-get
</div>

* Vérifiez les paramètres du pare-feu.
* Si vous ne pouvez pas accéder au dépôt pour une raison quelconque, téléchargez les paquets comme décrit dans l’article [guide d&#39;installation](../getting-started/install.md), puis installez-les manuellement à l’aide de la commande `sudo dpkg -i <packages>`. Vous aurez également besoin du paquet `tzdata`.

<div id="you-cannot-update-deb-packages-from-clickhouse-repository-with-apt-get">
  ### Vous ne pouvez pas mettre à jour les paquets deb du dépôt ClickHouse avec apt-get
</div>

* Le problème peut survenir lorsque la clé GPG a changé.

Veuillez suivre les instructions de la page [configuration](../getting-started/install.md#setup-the-debian-repository) pour mettre à jour la configuration du dépôt.

<div id="you-get-different-warnings-with-apt-get-update">
  ### Vous obtenez différents avertissements lors de `apt-get update`
</div>

* Les messages d’avertissement affichés peuvent être de l’un des types suivants :

```bash
N: Skipping acquire of configured file 'main/binary-i386/Packages' as repository 'https://packages.clickhouse.com/deb stable InRelease' doesn't support architecture 'i386'
```

```bash
E: Failed to fetch https://packages.clickhouse.com/deb/dists/stable/main/binary-amd64/Packages.gz  File has unexpected size (30451 != 28154). Mirror sync in progress?
```

```text
E: Repository 'https://packages.clickhouse.com/deb stable InRelease' changed its 'Origin' value from 'Artifactory' to 'ClickHouse'
E: Repository 'https://packages.clickhouse.com/deb stable InRelease' changed its 'Label' value from 'Artifactory' to 'ClickHouse'
N: Repository 'https://packages.clickhouse.com/deb stable InRelease' changed its 'Suite' value from 'stable' to ''
N: This must be accepted explicitly before updates for this repository can be applied. See apt-secure(8) manpage for details.
```

```bash
Err:11 https://packages.clickhouse.com/deb stable InRelease
  400  Bad Request [IP: 172.66.40.249 443]
```

Pour résoudre le problème ci-dessus, veuillez utiliser le script suivant :

```bash
sudo rm /var/lib/apt/lists/packages.clickhouse.com_* /var/lib/dpkg/arch /var/lib/apt/lists/partial/packages.clickhouse.com_*
sudo apt-get clean
sudo apt-get autoclean
```

<div id="you-cant-get-packages-with-yum-because-of-wrong-signature">
  ### Vous ne pouvez pas récupérer de paquets avec yum à cause d’une signature incorrecte
</div>

Problème possible : le cache est incorrect ; il a peut-être été corrompu après la mise à jour de la clé GPG en 2022-09.

La solution consiste à vider le cache et le répertoire lib de yum :

```bash
sudo find /var/lib/yum/repos/ /var/cache/yum/ -name 'clickhouse-*' -type d -exec rm -rf {} +
sudo rm -f /etc/yum.repos.d/clickhouse.repo
```

Ensuite, suivez le [guide d’installation](../getting-started/install.md#from-rpm-packages)

<div id="you-cant-run-docker-container">
  ### Vous ne pouvez pas lancer le conteneur Docker
</div>

Vous lancez simplement `docker run clickhouse/clickhouse-server` et il plante avec une stack trace semblable à la suivante :

```bash
$ docker run -it clickhouse/clickhouse-server
........
Poco::Exception. Code: 1000, e.code() = 0, System exception: cannot start thread, Stack trace (when copying this message, always include the lines below):

0. Poco::ThreadImpl::startImpl(Poco::SharedPtr<Poco::Runnable, Poco::ReferenceCounter, Poco::ReleasePolicy<Poco::Runnable>>) @ 0x00000000157c7b34
1. Poco::Thread::start(Poco::Runnable&) @ 0x00000000157c8a0e
2. BaseDaemon::initializeTerminationAndSignalProcessing() @ 0x000000000d267a14
3. BaseDaemon::initialize(Poco::Util::Application&) @ 0x000000000d2652cb
4. DB::Server::initialize(Poco::Util::Application&) @ 0x000000000d128b38
5. Poco::Util::Application::run() @ 0x000000001581cfda
6. DB::Server::run() @ 0x000000000d1288f0
7. Poco::Util::ServerApplication::run(int, char**) @ 0x0000000015825e27
8. mainEntryClickHouseServer(int, char**) @ 0x000000000d125b38
9. main @ 0x0000000007ea4eee
10. ? @ 0x00007f67ff946d90
11. ? @ 0x00007f67ff946e40
12. _start @ 0x00000000062e802e
 (version 24.10.1.2812 (official build))
```

La cause est une ancienne version du démon Docker, inférieure à `20.10.10`. Pour corriger le problème, vous pouvez soit le mettre à niveau, soit exécuter `docker run [--privileged | --security-opt seccomp=unconfined]`. Cette dernière option présente des implications en matière de sécurité.

<div id="troubleshooting-accepts-no-connections">
  ## Connexion au serveur
</div>

Problèmes possibles :

* Le serveur n’est pas démarré.
* Paramètres de configuration inattendus ou incorrects.

<div id="server-is-not-running">
  ### Le serveur n’est pas démarré
</div>

**Vérifiez que le serveur est démarré**

Commande :

```bash
$ sudo service clickhouse-server status
```

Si le serveur n’est pas lancé, lancez-le avec la commande :

```bash
$ sudo service clickhouse-server start
```

**Consulter les logs**

Le log principal de `clickhouse-server` se trouve par défaut dans `/var/log/clickhouse-server/clickhouse-server.log`.

Si le serveur a démarré correctement, vous devriez voir les chaînes suivantes :

* `<Information> Application: starting up.` — Le serveur a démarré.
* `<Information> Application: Ready for connections.` — Le serveur est en cours d’exécution et prêt à accepter des connexions.

Si le démarrage de `clickhouse-server` a échoué en raison d’une erreur de configuration, vous devriez voir la chaîne `<Error>` accompagnée d’une description de l’erreur. Par exemple :

```text
2019.01.11 15:23:25.549505 [ 45 ] {} <Error> ExternalDictionaries: Failed reloading 'event2id' external dictionary: Poco::Exception. Code: 1000, e.code() = 111, e.displayText() = Connection refused, e.what() = Connection refused
```

Si vous ne voyez pas d’erreur à la fin du fichier, parcourez l’intégralité du fichier à partir de la chaîne :

```text
<Information> Application: starting up.
```

Si vous essayez de démarrer une deuxième instance de `clickhouse-server` sur le serveur, le message suivant s&#39;affiche dans le journal :

```text
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

**Consulter les logs de system.d**

Si vous ne trouvez aucune information utile dans les logs de `clickhouse-server`, ou s’il n’y en a pas, vous pouvez consulter les logs de `system.d` à l’aide de la commande :

```bash
$ sudo journalctl -u clickhouse-server
```

**Démarrer clickhouse-server en mode interactif**

```bash
$ sudo -u clickhouse /usr/bin/clickhouse-server --config-file /etc/clickhouse-server/config.xml
```

Cette commande démarre le serveur en mode interactif avec les paramètres standard du script de démarrage automatique. Dans ce mode, `clickhouse-server` affiche tous les messages d’événements dans la console.

<div id="configuration-parameters">
  ### Paramètres de configuration
</div>

Vérifiez :

* Les paramètres Docker.

  Si vous exécutez ClickHouse dans Docker sur un réseau IPv6, assurez-vous que `network=host` est défini.

* Les paramètres de l’endpoint.

  Vérifiez les paramètres [listen&#95;host](../operations/server-configuration-parameters/settings.md#listen_host) et [tcp&#95;port](../operations/server-configuration-parameters/settings.md#tcp_port).

  Par défaut, ClickHouse server n’accepte que les connexions localhost.

* Les paramètres du protocole HTTP.

  Vérifiez les paramètres du protocole pour l’API HTTP.

* Les paramètres de connexion sécurisée.

  Vérifiez :

  * Le paramètre [tcp&#95;port&#95;secure](../operations/server-configuration-parameters/settings.md#tcp_port_secure).
  * Les paramètres des [certificats SSL](../operations/server-configuration-parameters/settings.md#openssl).

    Utilisez les paramètres appropriés lors de la connexion. Par exemple, utilisez le paramètre `port_secure` avec `clickhouse_client`.

* Les paramètres utilisateur.

  Il se peut que vous utilisiez un nom d’utilisateur ou un mot de passe incorrect.

<div id="troubleshooting-does-not-process-queries">
  ## Traitement des requêtes
</div>

Si ClickHouse ne parvient pas à traiter la requête, il envoie une description de l’erreur au client. Avec `clickhouse-client`, la description de l’erreur s’affiche dans la console. Si vous utilisez l’interface HTTP, ClickHouse envoie la description de l’erreur dans le corps de la réponse. Par exemple :

```bash
$ curl 'http://localhost:8123/' --data-binary "SELECT a"
Code: 47, e.displayText() = DB::Exception: Unknown identifier: a. Note that there are no tables (FROM clause) in your query, context: required_names: 'a' source_tables: table_aliases: private_aliases: column_aliases: public_columns: 'a' masked_columns: array_join_columns: source_columns: , e.what() = DB::Exception
```

Si vous démarrez `clickhouse-client` avec le paramètre `stack-trace`, ClickHouse renvoie la stack trace du serveur accompagnée de la description de l’erreur.

Il se peut que vous voyiez un message indiquant une connexion rompue. Dans ce cas, vous pouvez relancer la requête. Si la connexion se rompt chaque fois que vous exécutez la requête, vérifiez les logs du serveur pour y rechercher des erreurs.

<div id="troubleshooting-too-slow">
  ## Efficacité du traitement des requêtes
</div>

Si vous constatez que ClickHouse fonctionne trop lentement, vous devez analyser la charge de vos requêtes sur les ressources du serveur et sur le réseau.

Vous pouvez utiliser l&#39;utilitaire clickhouse-benchmark pour profiler les requêtes. Il affiche le nombre de requêtes traitées par seconde, le nombre de lignes traitées par seconde et les percentiles des temps de traitement des requêtes.