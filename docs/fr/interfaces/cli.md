---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 17
toc_title: Client De Ligne De Commande
---

# Client de ligne de commande {#command-line-client}

ClickHouse fournit un client de ligne de commande natif: `clickhouse-client`. Le client prend en charge les options de ligne de commande et les fichiers de configuration. Pour plus d'informations, voir [Configuration](#interfaces_cli_configuration).

[Installer](../getting-started/index.md) à partir de la `clickhouse-client` package et l'exécuter avec la commande `clickhouse-client`.

``` bash
$ clickhouse-client
ClickHouse client version 19.17.1.1579 (official build).
Connecting to localhost:9000 as user default.
Connected to ClickHouse server version 19.17.1 revision 54428.

:)
```

Différentes versions client et serveur sont compatibles les unes avec les autres, mais certaines fonctionnalités peuvent ne pas être disponibles dans les anciens clients. Nous vous recommandons d'utiliser la même version du client que l'application serveur. Lorsque vous essayez d'utiliser un client de l'ancienne version, puis le serveur, `clickhouse-client` affiche le message:

      ClickHouse client version is older than ClickHouse server. It may lack support for new features.

## Utilisation {#cli_usage}

Le client peut être utilisé en mode interactif et non interactif (batch). Pour utiliser le mode batch, spécifiez ‘query’ d'un paramètre ou d'envoyer des données à ‘stdin’ (il vérifie que ‘stdin’ n'est pas un terminal), ou les deux. Similaire à L'interface HTTP, lors de l'utilisation ‘query’ paramètre et envoi de données à ‘stdin’ la demande est une concaténation de la ‘query’ paramètre, un saut de ligne, et les données de ‘stdin’. Ceci est pratique pour les grandes requêtes D'insertion.

Exemple d'utilisation du client pour insérer des données:

``` bash
$ echo -ne "1, 'some text', '2016-08-14 00:00:00'\n2, 'some more text', '2016-08-14 00:00:01'" | clickhouse-client --database=test --query="INSERT INTO test FORMAT CSV";

$ cat <<_EOF | clickhouse-client --database=test --query="INSERT INTO test FORMAT CSV";
3, 'some text', '2016-08-14 00:00:00'
4, 'some more text', '2016-08-14 00:00:01'
_EOF

$ cat file.csv | clickhouse-client --database=test --query="INSERT INTO test FORMAT CSV";
```

En mode batch, le format de données par défaut est TabSeparated. Vous pouvez définir le format dans la clause FORMAT de la requête.

Par défaut, vous ne pouvez traiter une seule requête en mode batch. De faire plusieurs requêtes à partir d'un “script,” l'utilisation de la `--multiquery` paramètre. Cela fonctionne pour toutes les requêtes sauf INSERT. Les résultats de la requête sont produits consécutivement sans séparateurs supplémentaires. De même, pour traiter un grand nombre de requêtes, vous pouvez exécuter ‘clickhouse-client’ pour chaque requête. Notez que cela peut prendre des dizaines de millisecondes pour lancer le ‘clickhouse-client’ programme.

En mode interactif, vous obtenez une ligne de commande où vous pouvez entrer des requêtes.

Si ‘multiline’ n'est pas spécifié (par défaut): Pour exécuter la requête, appuyez sur Entrée. Le point-virgule n'est pas nécessaire à la fin de la requête. Pour entrer une requête multiligne, entrez une barre oblique inverse `\` avant le saut de ligne. Après avoir appuyé sur Entrée, il vous sera demandé d'entrer la ligne suivante de la requête.

Si multiline est spécifié: pour exécuter une requête, terminez-la par un point-virgule et appuyez sur Entrée. Si le point-virgule a été omis à la fin de l'entrée ligne, vous serez invité à entrer la ligne suivante de la requête.

Une seule requête est exécutée, donc tout après le point-virgule est ignoré.

Vous pouvez spécifier `\G` au lieu de le ou après le point-virgule. Cela indique le format Vertical. Dans ce format, chaque valeur est imprimée sur une ligne distincte, ce qui est pratique pour les grandes tables. Cette fonctionnalité inhabituelle a été ajoutée pour la compatibilité avec la CLI MySQL.

La ligne de commande est basé sur ‘replxx’ (similaire à ‘readline’). En d'autres termes, il utilise les raccourcis clavier familiers et conserve un historique. L'histoire est écrite à `~/.clickhouse-client-history`.

Par défaut, le format utilisé est Jolicompact. Vous pouvez modifier le format dans la clause FORMAT de la requête, ou en spécifiant `\G` à la fin de la requête, en utilisant le `--format` ou `--vertical` dans la ligne de commande, ou en utilisant le fichier de configuration client.

Pour quitter le client, appuyez sur Ctrl + D (ou Ctrl + C), ou entrez l'une des options suivantes au lieu d'une requête: “exit”, “quit”, “logout”, “exit;”, “quit;”, “logout;”, “q”, “Q”, “:q”

Lors du traitement d'une requête, le client affiche:

1.  Progrès, qui est mis à jour pas plus de 10 fois par seconde (par défaut). Pour les requêtes rapides, la progression peut ne pas avoir le temps d'être affichée.
2.  La requête formatée après l'analyse, pour le débogage.
3.  Le résultat dans le format spécifié.
4.  Le nombre de lignes dans le résultat, le temps passé et la vitesse moyenne de traitement des requêtes.

Vous pouvez annuler une requête longue en appuyant sur Ctrl + C. Cependant, vous devrez toujours attendre un peu pour que le serveur abandonne la requête. Il n'est pas possible d'annuler une requête à certaines étapes. Si vous n'attendez pas et appuyez une seconde fois sur Ctrl+C, le client quittera.

Le client de ligne de commande permet de passer des données externes (tables temporaires externes) pour l'interrogation. Pour plus d'informations, consultez la section “External data for query processing”.

### Requêtes avec paramètres {#cli-queries-with-parameters}

Vous pouvez créer une requête avec des paramètres et leur transmettre des valeurs à partir de l'application cliente. Cela permet d'éviter le formatage de la requête avec des valeurs dynamiques spécifiques côté client. Exemple:

``` bash
$ clickhouse-client --param_parName="[1, 2]"  -q "SELECT * FROM table WHERE a = {parName:Array(UInt16)}"
```

#### La Syntaxe De La Requête {#cli-queries-with-parameters-syntax}

Formatez une requête comme d'habitude, puis placez les valeurs que vous souhaitez transmettre des paramètres de l'application à la requête entre accolades au format suivant:

``` sql
{<name>:<data type>}
```

-   `name` — Placeholder identifier. In the console client it should be used in app parameters as `--param_<name> = value`.
-   `data type` — [Type de données](../sql-reference/data-types/index.md) de l'application valeur de paramètre. Par exemple, une structure de données comme `(integer, ('string', integer))` peut avoir la `Tuple(UInt8, Tuple(String, UInt8))` type de données (vous pouvez également utiliser un autre [entier](../sql-reference/data-types/int-uint.md) type).

#### Exemple {#example}

``` bash
$ clickhouse-client --param_tuple_in_tuple="(10, ('dt', 10))" -q "SELECT * FROM table WHERE val = {tuple_in_tuple:Tuple(UInt8, Tuple(String, UInt8))}"
```

## Configuration {#interfaces_cli_configuration}

Vous pouvez passer des paramètres à `clickhouse-client` (tous les paramètres ont une valeur par défaut) en utilisant:

-   À partir de la ligne de commande

    Les options de ligne de commande remplacent les valeurs et les paramètres par défaut dans les fichiers de configuration.

-   Les fichiers de Configuration.

    Les paramètres des fichiers de configuration remplacent les valeurs par défaut.

### Options De Ligne De Commande {#command-line-options}

-   `--host, -h` -– The server name, ‘localhost’ par défaut. Vous pouvez utiliser le nom ou L'adresse IPv4 ou IPv6.
-   `--port` – The port to connect to. Default value: 9000. Note that the HTTP interface and the native interface use different ports.
-   `--user, -u` – The username. Default value: default.
-   `--password` – The password. Default value: empty string.
-   `--query, -q` – The query to process when using non-interactive mode.
-   `--database, -d` – Select the current default database. Default value: the current database from the server settings (‘default’ par défaut).
-   `--multiline, -m` – If specified, allow multiline queries (do not send the query on Enter).
-   `--multiquery, -n` – If specified, allow processing multiple queries separated by semicolons.
-   `--format, -f` – Use the specified default format to output the result.
-   `--vertical, -E` – If specified, use the Vertical format by default to output the result. This is the same as ‘–format=Vertical’. Dans ce format, chaque valeur est imprimée sur une ligne séparée, ce qui est utile lors de l'affichage de tables larges.
-   `--time, -t` – If specified, print the query execution time to ‘stderr’ en mode non interactif.
-   `--stacktrace` – If specified, also print the stack trace if an exception occurs.
-   `--config-file` – The name of the configuration file.
-   `--secure` – If specified, will connect to server over secure connection.
-   `--param_<name>` — Value for a [requête avec paramètres](#cli-queries-with-parameters).

### Fichiers De Configuration {#configuration_files}

`clickhouse-client` utilise le premier fichier existant de suite:

-   Défini dans le `--config-file` paramètre.
-   `./clickhouse-client.xml`
-   `~/.clickhouse-client/config.xml`
-   `/etc/clickhouse-client/config.xml`

Exemple de fichier de configuration:

``` xml
<config>
    <user>username</user>
    <password>password</password>
    <secure>False</secure>
</config>
```

[Article Original](https://clickhouse.tech/docs/en/interfaces/cli/) <!--hide-->
