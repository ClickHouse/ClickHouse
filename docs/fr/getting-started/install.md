---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 11
toc_title: Installation
---

# Installation {#installation}

## Configuration Système Requise {#system-requirements}

ClickHouse peut fonctionner sur N'importe quel Linux, FreeBSD ou Mac OS X avec une architecture CPU x86\_64, AArch64 ou PowerPC64LE.

Les binaires pré-construits officiels sont généralement compilés pour le jeu d'instructions x86\_64 et leverage SSE 4.2, donc sauf indication contraire, l'utilisation du processeur qui le prend en charge devient une exigence système supplémentaire. Voici la commande pour vérifier si le processeur actuel prend en charge SSE 4.2:

``` bash
$ grep -q sse4_2 /proc/cpuinfo && echo "SSE 4.2 supported" || echo "SSE 4.2 not supported"
```

Pour exécuter ClickHouse sur des processeurs qui ne prennent pas en charge SSE 4.2 ou qui ont une architecture AArch64 ou PowerPC64LE, vous devez [construire ClickHouse à partir de sources](#from-sources) avec des ajustements de configuration appropriés.

## Options D'Installation Disponibles {#available-installation-options}

### À partir de paquets DEB {#install-from-deb-packages}

Il est recommandé d'utiliser officiel pré-compilé `deb` Paquets Pour Debian ou Ubuntu. Exécutez ces commandes pour installer les paquets:

``` bash
{% include 'install/deb.sh' %}
```

Si vous souhaitez utiliser la version la plus récente, remplacer `stable` avec `testing` (ceci est recommandé pour vos environnements de test).

Vous pouvez également télécharger et installer des paquets manuellement à partir de [ici](https://repo.clickhouse.tech/deb/stable/main/).

#### Paquet {#packages}

-   `clickhouse-common-static` — Installs ClickHouse compiled binary files.
-   `clickhouse-server` — Creates a symbolic link for `clickhouse-server` et installe la configuration du serveur par défaut.
-   `clickhouse-client` — Creates a symbolic link for `clickhouse-client` et d'autres outils. et installe les fichiers de configuration du client.
-   `clickhouse-common-static-dbg` — Installs ClickHouse compiled binary files with debug info.

### À partir de paquets RPM {#from-rpm-packages}

Il est recommandé d'utiliser officiel pré-compilé `rpm` packages pour CentOS, RedHat et toutes les autres distributions Linux basées sur rpm.

Tout d'abord, vous devez ajouter le dépôt officiel:

``` bash
sudo yum install yum-utils
sudo rpm --import https://repo.clickhouse.tech/CLICKHOUSE-KEY.GPG
sudo yum-config-manager --add-repo https://repo.clickhouse.tech/rpm/stable/x86_64
```

Si vous souhaitez utiliser la version la plus récente, remplacer `stable` avec `testing` (ceci est recommandé pour vos environnements de test). Le `prestable` la balise est parfois trop.

Exécutez ensuite ces commandes pour installer les paquets:

``` bash
sudo yum install clickhouse-server clickhouse-client
```

Vous pouvez également télécharger et installer des paquets manuellement à partir de [ici](https://repo.clickhouse.tech/rpm/stable/x86_64).

### À Partir D'Archives Tgz {#from-tgz-archives}

Il est recommandé d'utiliser officiel pré-compilé `tgz` archives pour toutes les distributions Linux, où l'installation de `deb` ou `rpm` les emballages n'est pas possible.

La version requise peut être téléchargée avec `curl` ou `wget` depuis le référentiel https://repo.clickhouse.tech/tgz/.
Après cela, les archives téléchargées doivent être décompressées et installées avec des scripts d'installation. Exemple pour la dernière version:

``` bash
export LATEST_VERSION=`curl https://api.github.com/repos/ClickHouse/ClickHouse/tags 2>/dev/null | grep -Eo '[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+' | head -n 1`
curl -O https://repo.clickhouse.tech/tgz/clickhouse-common-static-$LATEST_VERSION.tgz
curl -O https://repo.clickhouse.tech/tgz/clickhouse-common-static-dbg-$LATEST_VERSION.tgz
curl -O https://repo.clickhouse.tech/tgz/clickhouse-server-$LATEST_VERSION.tgz
curl -O https://repo.clickhouse.tech/tgz/clickhouse-client-$LATEST_VERSION.tgz

tar -xzvf clickhouse-common-static-$LATEST_VERSION.tgz
sudo clickhouse-common-static-$LATEST_VERSION/install/doinst.sh

tar -xzvf clickhouse-common-static-dbg-$LATEST_VERSION.tgz
sudo clickhouse-common-static-dbg-$LATEST_VERSION/install/doinst.sh

tar -xzvf clickhouse-server-$LATEST_VERSION.tgz
sudo clickhouse-server-$LATEST_VERSION/install/doinst.sh
sudo /etc/init.d/clickhouse-server start

tar -xzvf clickhouse-client-$LATEST_VERSION.tgz
sudo clickhouse-client-$LATEST_VERSION/install/doinst.sh
```

Pour les environnements de production, il est recommandé d'utiliser la dernière `stable`-version. Vous pouvez trouver son numéro sur la page GitHub https://github.com/ClickHouse/ClickHouse/tags avec postfix `-stable`.

### À Partir De L'Image Docker {#from-docker-image}

Pour exécuter Clickhouse à L'intérieur Docker suivez le guide sur [Hub Docker](https://hub.docker.com/r/yandex/clickhouse-server/). Ces images utilisent officiel `deb` les paquets à l'intérieur.

### À Partir De Sources {#from-sources}

Pour compiler manuellement ClickHouse, suivez les instructions pour [Linux](../development/build.md) ou [Mac OS X](../development/build-osx.md).

Vous pouvez compiler des paquets et les installer ou utiliser des programmes sans installer de paquets. En outre, en construisant manuellement, vous pouvez désactiver L'exigence SSE 4.2 ou construire pour les processeurs AArch64.

      Client: programs/clickhouse-client
      Server: programs/clickhouse-server

Vous devrez créer un dossier de données et de métadonnées et `chown` pour l'utilisateur souhaité. Leurs chemins peuvent être modifiés dans la configuration du serveur (src / programs / server / config.xml), par défaut, ils sont:

      /opt/clickhouse/data/default/
      /opt/clickhouse/metadata/default/

Sur Gentoo, vous pouvez simplement utiliser `emerge clickhouse` pour installer ClickHouse à partir de sources.

## Lancer {#launch}

Pour démarrer le serveur en tant que démon, exécutez:

``` bash
$ sudo service clickhouse-server start
```

Si vous n'avez pas `service` commande, exécuter comme

``` bash
$ sudo /etc/init.d/clickhouse-server start
```

Voir les journaux dans le `/var/log/clickhouse-server/` répertoire.

Si le serveur ne démarre pas, vérifiez les configurations dans le fichier `/etc/clickhouse-server/config.xml`.

Vous pouvez également lancer manuellement le serveur à partir de la console:

``` bash
$ clickhouse-server --config-file=/etc/clickhouse-server/config.xml
```

Dans ce cas, le journal sera imprimé sur la console, ce qui est pratique lors du développement.
Si le fichier de configuration se trouve dans le répertoire courant, vous n'avez pas besoin `--config-file` paramètre. Par défaut, il utilise `./config.xml`.

ClickHouse prend en charge les paramètres de restriction d'accès. Ils sont situés dans la `users.xml` fichier (à côté de `config.xml`).
Par défaut, l'accès est autorisé depuis n'importe où pour `default` l'utilisateur, sans un mot de passe. Voir `user/default/networks`.
Pour plus d'informations, consultez la section [“Configuration Files”](../operations/configuration-files.md).

Après le lancement du serveur, vous pouvez utiliser le client de ligne de commande pour vous y connecter:

``` bash
$ clickhouse-client
```

Par défaut, il se connecte à `localhost:9000` au nom de l'utilisateur `default` sans un mot de passe. Il peut également être utilisé pour se connecter à un serveur distant en utilisant `--host` argument.

Le terminal doit utiliser L'encodage UTF-8.
Pour plus d'informations, consultez la section [“Command-line client”](../interfaces/cli.md).

Exemple:

``` bash
$ ./clickhouse-client
ClickHouse client version 0.0.18749.
Connecting to localhost:9000.
Connected to ClickHouse server version 0.0.18749.

:) SELECT 1

SELECT 1

┌─1─┐
│ 1 │
└───┘

1 rows in set. Elapsed: 0.003 sec.

:)
```

**Félicitations, le système fonctionne!**

Pour continuer à expérimenter, vous pouvez télécharger l'un des jeux de données de test ou passer par [tutoriel](https://clickhouse.tech/tutorial.html).

[Article Original](https://clickhouse.tech/docs/en/getting_started/install/) <!--hide-->
