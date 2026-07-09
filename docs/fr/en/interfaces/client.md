---
description: 'Documentation de l’interface en ligne de commande ClickHouse'
sidebar_label: 'ClickHouse Client'
sidebar_position: 18
slug: /interfaces/client
title: 'ClickHouse Client'
doc_type: 'reference'
---

import Image from '@theme/IdealImage';
import cloud_connect_button from '@site/static/images/_snippets/cloud-connect-button.png';
import connection_details_native from '@site/static/images/_snippets/connection-details-native.png';
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

ClickHouse fournit un client en ligne de commande natif permettant d’exécuter des requêtes SQL directement sur un serveur ClickHouse.
Il prend en charge à la fois le mode interactif (pour l’exécution de requêtes en direct) et le mode batch (pour les scripts et l’automatisation).
Le résultat de la requête peut être affiché dans le terminal ou exporté vers un fichier, avec prise en charge de tous les [formats](formats.md) de sortie ClickHouse, tels que Pretty, CSV, JSON, etc.

Le client fournit des informations en temps réel sur l’exécution des requêtes, avec une barre de progression, le nombre de lignes lues, le nombre d’octets traités et le temps d’exécution de la requête.
Il prend en charge à la fois les [options de ligne de commande](#command-line-options) et les [fichiers de configuration](#configuration_files).

<div id="install">
  ## Installation
</div>

Pour télécharger ClickHouse, exécutez :

```bash
curl https://clickhouse.com/ | sh
```

Pour l’installer également, exécutez :

```bash
sudo ./clickhouse install
```

Consultez [Installer ClickHouse](../getting-started/install/install.mdx) pour découvrir d&#39;autres options d&#39;installation.

Différentes versions du client et du serveur sont compatibles entre elles, mais certaines fonctionnalités peuvent ne pas être disponibles dans les anciens clients. Nous vous recommandons d&#39;utiliser la même version pour le client et le serveur.

<div id="run">
  ## Exécuter
</div>

:::note
Si vous avez téléchargé ClickHouse sans l’installer, utilisez `./clickhouse client` au lieu de `clickhouse-client`.
:::

Pour vous connecter à un serveur ClickHouse, exécutez :

```bash
$ clickhouse-client --host server

ClickHouse client version 24.12.2.29 (official build).
Connecting to server:9000 as user default.
Connected to ClickHouse server version 24.12.2.

:)
```

Spécifiez des informations de connexion supplémentaires si nécessaire :

| Option                           | Description                                                                                                                                                                                                      |
| -------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `--port <port>`                  | Le port sur lequel serveur ClickHouse accepte les connexions. Les ports par défaut sont 9440 (TLS) et 9000 (sans TLS). Notez que ClickHouse Client utilise le protocole natif, et non HTTP(S).                    |
| `-s [ --secure ]`                | Indique s’il faut utiliser TLS (généralement détecté automatiquement).                                                                                                                                           |
| `-u [ --user ] <username>`       | L’utilisateur de base de données avec lequel se connecter. Par défaut, la connexion s’effectue avec l’utilisateur `default`.                                                                                     |
| `--password <password>`          | Le mot de passe de l’utilisateur de base de données. Vous pouvez également spécifier le mot de passe d’une connexion dans le fichier de configuration. Si vous ne le spécifiez pas, le client vous le demandera. |
| `-c [ --config ] <path-to-file>` | L’emplacement du fichier de configuration de ClickHouse Client, s’il ne se trouve pas dans l’un des emplacements par défaut. Voir [Fichiers de configuration](#configuration_files).                             |
| `--connection <name>`            | Le nom des informations de connexion préconfigurées issues du [fichier de configuration](#connection-credentials).                                                                                               |

Pour obtenir la liste complète des options en ligne de commande, voir [Options de ligne de commande](#command-line-options).

<div id="connecting-cloud">
  ### Se connecter à ClickHouse Cloud
</div>

Les informations de votre service ClickHouse Cloud sont disponibles dans la console ClickHouse Cloud. Sélectionnez le service auquel vous souhaitez vous connecter, puis cliquez sur **Connect** :

<Image img={cloud_connect_button} size="md" alt="Bouton Connect du service ClickHouse Cloud" />

<br />

<br />

Choisissez **Native** pour afficher les informations, avec un exemple de commande `clickhouse-client` :

<Image img={connection_details_native} size="md" alt="Détails de la connexion TCP native du service ClickHouse Cloud" />

<div id="connection-credentials">
  ### Stocker des connexions dans un fichier de configuration
</div>

Vous pouvez enregistrer les informations de connexion d’un ou de plusieurs serveurs ClickHouse dans un [fichier de configuration](#configuration_files).

Le format est le suivant :

```xml
<config>
    <connections_credentials>
        <connection>
            <name>default</name>
            <hostname>hostname</hostname>
            <port>9440</port>
            <secure>1</secure>
            <user>default</user>
            <password>password</password>
            <!-- <history_file></history_file> -->
            <!-- <history_max_entries></history_max_entries> -->
            <!-- <accept-invalid-certificate>false</accept-invalid-certificate> -->
            <!-- <prompt></prompt> -->
        </connection>
    </connections_credentials>
</config>
```

Voir la [section sur les fichiers de configuration](#configuration_files) pour plus d’informations.

:::note
Pour mettre l’accent sur la syntaxe des requêtes, les exemples ci-dessous omettent les détails de connexion (`--host`, `--port`, etc.). N’oubliez pas de les ajouter lorsque vous utilisez ces commandes.
:::

<div id="interactive-mode">
  ## Mode interactif
</div>

<div id="using-interactive-mode">
  ### Utiliser le mode interactif
</div>

Pour exécuter ClickHouse en mode interactif, lancez simplement :

```bash
clickhouse-client
```

Cela ouvre la boucle REPL (Read-Eval-Print Loop), dans laquelle vous pouvez commencer à saisir des requêtes SQL de façon interactive.
Une fois connecté, une invite de commande vous permet de saisir des requêtes :

```bash
ClickHouse client version 25.x.x.x
Connecting to localhost:9000 as user default.
Connected to ClickHouse server version 25.x.x.x

hostname :)
```

En mode interactif, le format de sortie par défaut est `PrettyCompact`.
Vous pouvez changer de format dans la clause `FORMAT` de la requête ou en spécifiant l&#39;option de ligne de commande `--format`.
Pour utiliser le format Vertical, vous pouvez utiliser `--vertical` ou ajouter `\G` à la fin de la requête.
Dans ce format, chaque valeur est affichée sur une ligne distincte, ce qui est pratique pour les tables larges.

En mode interactif, par défaut, tout ce que vous saisissez est exécuté lorsque vous appuyez sur `Enter`.
Il n&#39;est pas nécessaire d&#39;ajouter un point-virgule à la fin de la requête.

Vous pouvez démarrer le client avec le paramètre `-m, --multiline`.
Pour saisir une requête multiligne, entrez une barre oblique inverse `\` avant le saut de ligne.
Après avoir appuyé sur `Enter`, vous serez invité à saisir la ligne suivante de la requête.
Pour exécuter la requête, terminez-la par un point-virgule et appuyez sur `Enter`.

ClickHouse Client repose sur `replxx` (similaire à `readline`) et utilise donc des raccourcis clavier familiers tout en conservant un historique.
Par défaut, l&#39;historique est enregistré dans `~/.clickhouse-client-history`.

Pour quitter le client, appuyez sur `Ctrl+D` ou saisissez l&#39;une des commandes suivantes à la place d&#39;une requête :

* `exit` ou `exit;`
* `quit` ou `quit;`
* `q`, `Q` ou `:q`
* `logout` ou `logout;`

<div id="getting-help">
  ### Obtenir de l’aide
</div>

Vous pouvez consulter la documentation de n’importe quelle fonction, moteur de table, type de données, format, paramètre ou autre composant du système sans quitter le client. Saisissez `help` suivi d’un nom (les formes équivalentes `/help`, `man` et `/man` fonctionnent également) :

```text
help domainWithoutWWW
```

La recherche est insensible à la casse et interroge la table [`system.documentation`](../operations/system-tables/documentation.md). La documentation correspondante est affichée dans le terminal à partir du Markdown, avec du texte en gras/italique, des tableaux et des blocs de code avec coloration syntaxique. Lorsqu’un même nom correspond à plusieurs composants (par exemple `file`, qui est à la fois une fonction et un moteur de table), ils sont tous affichés.

Lorsqu’aucune correspondance exacte n’est trouvée, le client affiche une liste de noms similaires (en tolérant les fautes de frappe) ainsi que des composants dont la documentation mentionne le mot :

```text
help maxx_threads
```

Saisir simplement `help` affiche un bref résumé d&#39;utilisation.

<div id="processing-info">
  ### Informations sur le traitement des requêtes
</div>

Lors du traitement d’une requête, le client affiche :

1. La progression, qui n’est par défaut mise à jour qu’au plus 10 fois par seconde.
   Pour les requêtes rapides, elle peut ne pas avoir le temps de s’afficher.
2. La requête mise en forme après l’analyse, à des fins de débogage.
3. Le résultat dans le format spécifié.
4. Le nombre de lignes du résultat, le temps écoulé et la vitesse moyenne de traitement de la requête.
   Toutes les quantités de données se rapportent aux données non compressées.

Vous pouvez annuler une requête longue en appuyant sur `Ctrl+C`.
Cependant, vous devrez tout de même attendre un peu que le serveur abandonne la requête.
Il n’est pas possible d’annuler une requête à certaines étapes.
Si vous n’attendez pas et appuyez une seconde fois sur `Ctrl+C`, le client se fermera.

ClickHouse Client permet de transmettre des données externes (tables temporaires externes) pour l’exécution de requêtes.
Pour plus d’informations, consultez la section [Données externes pour le traitement des requêtes](../engines/table-engines/special/external-data.md).

<div id="cli_aliases">
  ### Alias
</div>

Vous pouvez utiliser les alias suivants dans le REPL :

* `\l` - SHOW DATABASES
* `\d` - SHOW TABLES
* `\c <DATABASE>` - USE DATABASE
* `.` - répète la dernière requête

<div id="keyboard_shortcuts">
  ### Raccourcis clavier
</div>

* `Alt (Option) + Shift + e` - ouvre l’éditeur avec la requête en cours. Il est possible de spécifier l’éditeur à utiliser avec la variable d’environnement `EDITOR`. Par défaut, `vim` est utilisé.
* `Alt (Option) + #` - commente la ligne.
* `Ctrl + r` - recherche floue dans l’historique.

La liste complète de tous les raccourcis clavier disponibles est consultable sur [replxx](https://github.com/AmokHuginnsson/replxx/blob/1f149bf/src/replxx_impl.cxx#L262).

:::tip
Pour configurer correctement la touche méta (Option) sur MacOS :

iTerm2 : accédez à Preferences -&gt; Profile -&gt; Keys -&gt; Left Option key, puis cliquez sur Esc+
:::

<div id="batch-mode">
  ## Mode batch
</div>

<div id="using-batch-mode">
  ### Utiliser le mode batch
</div>

Au lieu d’utiliser ClickHouse Client en mode interactif, vous pouvez l’exécuter en mode batch.
En mode batch, ClickHouse exécute une seule requête puis se ferme immédiatement : il n’y a ni invite interactive ni boucle.

Vous pouvez spécifier une seule requête comme ceci :

```bash
$ clickhouse-client "SELECT sum(number) FROM numbers(10)"
45
```

Vous pouvez également utiliser l’option `--query` en ligne de commande :

```bash
$ clickhouse-client --query "SELECT uniq(number) FROM numbers(10)"
10
```

Vous pouvez fournir une requête via `stdin` :

```bash
$ echo "SELECT avg(number) FROM numbers(10)" | clickhouse-client
4.5
```

En supposant qu&#39;une table `messages` existe, vous pouvez également insérer des données en ligne de commande :

```bash
$ echo "Hello\nGoodbye" | clickhouse-client --query "INSERT INTO messages FORMAT CSV"
```

Lorsque `--query` est spécifié, toute entrée fournie est ajoutée à la requête après un saut de ligne.

<div id="cloud-example">
  ### Insertion d’un fichier CSV dans un service ClickHouse distant
</div>

Cet exemple insère le fichier CSV d’un jeu de données d’exemple, `cell_towers.csv`, dans la table existante `cell_towers` de la base de données `default` :

```bash
clickhouse-client --host HOSTNAME.clickhouse.cloud \
  --port 9440 \
  --user default \
  --password PASSWORD \
  --query "INSERT INTO cell_towers FORMAT CSVWithNames" \
  < cell_towers.csv
```

<div id="more-examples">
  ### Exemples d’insertion de données en ligne de commande
</div>

Il existe plusieurs façons d’insérer des données en ligne de commande.
L’exemple ci-dessous insère deux lignes au format CSV dans une table ClickHouse en mode batch :

```bash
echo -ne "1, 'some text', '2016-08-14 00:00:00'\n2, 'some more text', '2016-08-14 00:00:01'" | \
  clickhouse-client --database=test --query="INSERT INTO test FORMAT CSV";
```

Dans l’exemple ci-dessous, `cat <<_EOF` commence un heredoc qui lit tout ce qui suit jusqu’à ce qu’il rencontre de nouveau `_EOF`, puis l’affiche :

```bash
cat <<_EOF | clickhouse-client --database=test --query="INSERT INTO test FORMAT CSV";
3, 'some text', '2016-08-14 00:00:00'
4, 'some more text', '2016-08-14 00:00:01'
_EOF
```

Dans l’exemple ci-dessous, le contenu de file.csv est affiché sur stdout à l’aide de `cat`, puis passé en entrée à `clickhouse-client` via un pipe :

```bash
cat file.csv | clickhouse-client --database=test --query="INSERT INTO test FORMAT CSV";
```

En mode batch, le [format](formats.md) par défaut des données est `TabSeparated`.
Vous pouvez définir le format dans la clause `FORMAT` de la requête, comme dans l’exemple ci-dessus.

<div id="cli-queries-with-parameters">
  ## Requêtes paramétrées
</div>

Vous pouvez définir des paramètres dans une requête et leur transmettre des valeurs à l’aide d’options en ligne de commande.
Cela évite de formater côté client une requête avec des valeurs dynamiques spécifiques.
Par exemple :

```bash
$ clickhouse-client --param_parName="[1, 2]" --query "SELECT {parName: Array(UInt16)}"
[1,2]
```

Il est également possible de définir des paramètres dans une [session interactive](#interactive-mode) :

```text
$ clickhouse-client
ClickHouse client version 25.X.X.XXX (official build).

#highlight-next-line
:) SET param_parName='[1, 2]';

SET param_parName = '[1, 2]'

Query id: 7ac1f84e-e89a-4eeb-a4bb-d24b8f9fd977

Ok.

0 rows in set. Elapsed: 0.000 sec.

#highlight-next-line
:) SELECT {parName:Array(UInt16)}

SELECT {parName:Array(UInt16)}

Query id: 0358a729-7bbe-4191-bb48-29b063c548a7

   ┌─_CAST([1, 2]⋯y(UInt16)')─┐
1. │ [1,2]                    │
   └──────────────────────────┘

1 row in set. Elapsed: 0.006 sec.
```

<div id="cli-queries-with-parameters-syntax">
  ### Syntaxe de la requête
</div>

Dans la requête, placez entre accolades les valeurs que vous souhaitez renseigner à l’aide de paramètres de ligne de commande, au format suivant :

```sql
{<name>:<data type>}
```

| Paramètre   | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| ----------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `name`      | Identifiant de remplacement. L’option de ligne de commande correspondante est `--param_<name> = value`.                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| `data type` | [Type de données](../sql-reference/data-types/index.md) du paramètre. <br /><br />Par exemple, une structure de données comme `(integer, ('string', integer))` peut avoir pour type de données `Tuple(UInt8, Tuple(String, UInt8))` (vous pouvez également utiliser d’autres types [integer](../sql-reference/data-types/int-uint.md)). <br /><br />Il est également possible de passer en paramètres le nom de la table, le nom de la base de données et les noms de colonnes ; dans ce cas, vous devrez utiliser `Identifier` comme type de données. |

<div id="cli-queries-with-parameters-examples">
  ### Exemples
</div>

```bash
$ clickhouse-client --param_tuple_in_tuple="(10, ('dt', 10))" \
    --query "SELECT * FROM table WHERE val = {tuple_in_tuple:Tuple(UInt8, Tuple(String, UInt8))}"

$ clickhouse-client --param_tbl="numbers" --param_db="system" --param_col="number" --param_alias="top_ten" \
    --query "SELECT {col:Identifier} as {alias:Identifier} FROM {db:Identifier}.{tbl:Identifier} LIMIT 10"
```

<div id="ai-sql-generation">
  ## Génération de SQL par IA
</div>

ClickHouse Client intègre une assistance IA native pour générer des requêtes SQL à partir de descriptions en langage naturel. Cette fonctionnalité aide les utilisateurs à rédiger des requêtes complexes sans connaissance approfondie de SQL.

L’assistance IA fonctionne immédiatement si la variable d’environnement `OPENAI_API_KEY` ou `ANTHROPIC_API_KEY` est définie. Pour une configuration plus avancée, consultez la section [Configuration](#ai-sql-generation-configuration).

<div id="ai-sql-generation-usage">
  ### Utilisation
</div>

Pour utiliser la génération SQL par IA, faites précéder votre requête en langage naturel de `??` :

```bash
:) ?? show all users who made purchases in the last 30 days
```

L’IA va :

1. Analyser automatiquement le schéma de votre base de données
2. Générer la requête SQL appropriée à partir des tables et colonnes détectées
3. Exécuter immédiatement la requête générée

<div id="ai-sql-generation-example">
  ### Exemple
</div>

```bash
:) ?? count orders by product category

Starting AI SQL generation with schema discovery...
──────────────────────────────────────────────────

🔍 list_databases
   ➜ system, default, sales_db

🔍 list_tables_in_database
   database: sales_db
   ➜ orders, products, categories

🔍 get_schema_for_table
   database: sales_db
   table: orders
   ➜ CREATE TABLE orders (order_id UInt64, product_id UInt64, quantity UInt32, ...)

✨ SQL query generated successfully!
──────────────────────────────────────────────────

SELECT
    c.name AS category,
    COUNT(DISTINCT o.order_id) AS order_count
FROM sales_db.orders o
JOIN sales_db.products p ON o.product_id = p.product_id
JOIN sales_db.categories c ON p.category_id = c.category_id
GROUP BY c.name
ORDER BY order_count DESC
```

<div id="ai-sql-generation-configuration">
  ### Configuration
</div>

La génération SQL par IA nécessite de configurer un fournisseur d’IA dans le fichier de configuration de votre ClickHouse Client. Vous pouvez utiliser OpenAI, Anthropic ou tout service API compatible avec OpenAI.

<div id="ai-sql-generation-fallback">
  #### Mécanisme de secours basé sur l’environnement
</div>

Si aucune configuration d’IA n’est spécifiée dans le fichier de configuration, ClickHouse Client essaiera automatiquement d’utiliser les variables d’environnement :

1. Vérifie d’abord la variable d’environnement `OPENAI_API_KEY`
2. Si elle n’est pas trouvée, vérifie la variable d’environnement `ANTHROPIC_API_KEY`
3. Si aucune des deux n’est trouvée, les fonctionnalités d’IA seront désactivées

Cela permet une configuration rapide sans fichier de configuration :

```bash
# Using OpenAI
export OPENAI_API_KEY=your-openai-key
clickhouse-client

# Using Anthropic
export ANTHROPIC_API_KEY=your-anthropic-key
clickhouse-client
```

<div id="ai-sql-generation-configuration-file">
  #### Fichier de configuration
</div>

Pour mieux maîtriser les paramètres de l’IA, configurez-les dans le fichier de configuration de votre ClickHouse Client situé à :

* `$XDG_CONFIG_HOME/clickhouse/config.xml` (ou `~/.config/clickhouse/config.xml` si `XDG_CONFIG_HOME` n’est pas défini) (format XML)
* `$XDG_CONFIG_HOME/clickhouse/config.yaml` (ou `~/.config/clickhouse/config.yaml` si `XDG_CONFIG_HOME` n’est pas défini) (format YAML)
* `~/.clickhouse-client/config.xml` (format XML, emplacement legacy)
* `~/.clickhouse-client/config.yaml` (format YAML, emplacement legacy)
* Ou indiquez un emplacement personnalisé avec `--config-file`

<Tabs>
  <TabItem value="xml" label="XML" default>
    ```xml
    <config>
        <ai>
            <!-- Requis : votre clé API (ou définie via une variable d’environnement) -->
            <api_key>your-api-key-here</api_key>

            <!-- Requis : type de fournisseur (openai, anthropic) -->
            <provider>openai</provider>

            <!-- Modèle à utiliser (les valeurs par défaut varient selon le fournisseur) -->
            <model>gpt-4o</model>

            <!-- Facultatif : point de terminaison d’API personnalisé pour les services compatibles OpenAI -->
            <!-- <base_url>https://openrouter.ai/api</base_url> -->

            <!-- Paramètres d’exploration du schéma -->
            <enable_schema_access>true</enable_schema_access>

            <!-- Paramètres de génération -->
            <!-- Facultatif : temperature n’est envoyée au modèle que lorsqu’elle est définie ici.
                 Elle est omise par défaut, car certains modèles rejettent ce paramètre. -->
            <!-- <temperature>0.0</temperature> -->
            <max_tokens>1000</max_tokens>
            <timeout_seconds>30</timeout_seconds>
            <max_steps>10</max_steps>

            <!-- Facultatif : prompt système personnalisé -->
            <!-- <system_prompt>You are an expert ClickHouse SQL assistant...</system_prompt> -->
        </ai>
    </config>
    ```
  </TabItem>

  <TabItem value="yaml" label="YAML">
    ```yaml
    ai:
      # Requis : votre clé API (ou définie via une variable d’environnement)
      api_key: your-api-key-here

      # Requis : type de fournisseur (openai, anthropic)
      provider: openai

      # Modèle à utiliser
      model: gpt-4o

      # Facultatif : point de terminaison d’API personnalisé pour les services compatibles OpenAI
      # base_url: https://openrouter.ai/api

      # Activer l’accès au schéma - permet à l’IA d’interroger les informations sur les bases de données et les tables
      enable_schema_access: true

      # Paramètres de génération
      # temperature n’est envoyée au modèle que lorsqu’elle est définie ici ; elle est omise par défaut
      # car certains modèles rejettent ce paramètre.
      # temperature: 0.0    # Contrôle le caractère aléatoire (0.0 = déterministe)
      max_tokens: 1000      # Longueur maximale de la réponse
      timeout_seconds: 30   # Délai d’expiration de la requête
      max_steps: 10         # Nombre maximal d’étapes d’exploration du schéma

      # Facultatif : prompt système personnalisé
      # system_prompt: |
      #   You are an expert ClickHouse SQL assistant. Convert natural language to SQL.
      #   Focus on performance and use ClickHouse-specific optimizations.
      #   Always return executable SQL without explanations.
    ```
  </TabItem>
</Tabs>

<br />

**Utilisation d’API compatibles OpenAI (par ex. OpenRouter) :**

```yaml
ai:
  provider: openai  # Use 'openai' for compatibility
  api_key: your-openrouter-api-key
  base_url: https://openrouter.ai/api/v1
  model: anthropic/claude-3.5-sonnet  # Use OpenRouter model naming
```

**Exemples de configuration minimale :**

```yaml
# Minimal config - uses environment variable for API key
ai:
  provider: openai  # Will use OPENAI_API_KEY env var

# No config at all - automatic fallback
# (Empty or no ai section - will try OPENAI_API_KEY then ANTHROPIC_API_KEY)

# Only override model - uses env var for API key
ai:
  provider: openai
  model: gpt-3.5-turbo
```

<div id="ai-sql-generation-parameters">
  ### Paramètres
</div>

<details>
  <summary>Paramètres requis</summary>

  * `api_key` - Votre clé API pour le service d&#39;IA. Peut être omise si elle est définie via une variable d&#39;environnement :
    * OpenAI: `OPENAI_API_KEY`
    * Anthropic: `ANTHROPIC_API_KEY`
    * Remarque : la clé API du fichier de configuration est prioritaire sur la variable d&#39;environnement
  * `provider` - Le fournisseur d&#39;IA : `openai` ou `anthropic`
    * S&#39;il est omis, un mécanisme de secours automatique est effectué en fonction des variables d&#39;environnement disponibles
</details>

<details>
  <summary>Configuration du modèle</summary>

  * `model` - Le modèle à utiliser (par défaut : propre au fournisseur)
    * OpenAI: `gpt-4o`, `gpt-4`, `gpt-3.5-turbo`, etc.
    * Anthropic: `claude-3-5-sonnet-20241022`, `claude-3-opus-20240229`, etc.
    * OpenRouter: utilisez leur convention de nommage des modèles, par exemple `anthropic/claude-3.5-sonnet`
</details>

<details>
  <summary>Paramètres de connexion</summary>

  * `base_url` - point de terminaison d’API personnalisé pour les services compatibles OpenAI (facultatif)
  * `timeout_seconds` - Délai d&#39;expiration de la requête, en secondes (par défaut : `30`)
</details>

<details>
  <summary>Exploration des schémas</summary>

  * `enable_schema_access` - Autorise l&#39;IA à explorer les schémas de la base de données (par défaut : `true`)
  * `max_steps` - Nombre maximal d&#39;étapes d&#39;appel d&#39;outils pour l&#39;exploration des schémas (par défaut : `10`)
</details>

<details>
  <summary>Paramètres de génération</summary>

  * `temperature` - Contrôle le degré d&#39;aléatoire : 0.0 = déterministe, 1.0 = créatif. Ce paramètre est omis par défaut et n&#39;est envoyé au modèle que s&#39;il est explicitement défini, car certains modèles le rejettent.
  * `max_tokens` - Longueur maximale de la réponse en tokens (par défaut : `1000`)
  * `system_prompt` - Instructions personnalisées pour l&#39;IA (facultatif)
</details>

<div id="ai-sql-generation-how-it-works">
  ### Fonctionnement
</div>

Le générateur SQL par IA suit un processus en plusieurs étapes :

<VerticalStepper headerLevel="list">
  1. **Découverte du schéma**

  L’IA utilise des outils intégrés pour explorer votre base de données

  * Répertorie les bases de données disponibles
  * Découvre les tables dans les bases de données pertinentes
  * Inspecte la structure des tables à l’aide d’instructions `CREATE TABLE`

  2. **Génération de requêtes**

  À partir du schéma découvert, l’IA génère du SQL qui :

  * Correspond à votre intention en langage naturel
  * Utilise les noms corrects des tables et des colonnes
  * Applique les jointures et les agrégations appropriées

  3. **Exécution**

  Le SQL généré est exécuté automatiquement et les résultats s’affichent
</VerticalStepper>

<div id="ai-sql-generation-limitations">
  ### Limitations
</div>

* Nécessite une connexion Internet active
* L&#39;utilisation de l&#39;API est soumise aux limites de débit et aux coûts imposés par le fournisseur d&#39;IA
* Les requêtes complexes peuvent nécessiter plusieurs ajustements
* L&#39;IA n&#39;a qu&#39;un accès en lecture seule aux informations de schéma, pas aux données elles-mêmes

<div id="ai-sql-generation-security">
  ### Sécurité
</div>

* Les clés API ne sont jamais envoyées aux serveurs ClickHouse
* L’IA n’accède qu’aux informations de schéma (noms de tables/colonnes et types), pas aux données elles-mêmes
* Toutes les requêtes générées respectent les permissions existantes de votre base de données

<div id="connection_string">
  ## Chaîne de connexion
</div>

<div id="ai-sql-generation-usage">
  ### Utilisation
</div>

ClickHouse Client prend aussi en charge la connexion à un serveur ClickHouse à l’aide d’une chaîne de connexion similaire à celles de [MongoDB](https://www.mongodb.com/docs/manual/reference/connection-string/), [PostgreSQL](https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNSTRING) et [MySQL](https://dev.mysql.com/doc/refman/8.0/en/connecting-using-uri-or-key-value-pairs.html#connecting-using-uri). Sa syntaxe est la suivante :

```text
clickhouse:[//[user[:password]@][hosts_and_ports]][/database][?query_parameters]
```

| Composant (tous facultatifs) | Description                                                                                                                                                                               | Par défaut       |
| ---------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------- |
| `user`                       | Nom d’utilisateur de la base de données.                                                                                                                                                  | `default`        |
| `password`                   | Mot de passe de l’utilisateur de la base de données. Si `:` est spécifié et que le mot de passe est vide, le client invitera l’utilisateur à saisir son mot de passe.                     | -                |
| `hosts_and_ports`            | Liste d’hôtes et de ports facultatifs `host[:port] [, host:[port]], ...`.                                                                                                                 | `localhost:9000` |
| `database`                   | Nom de la base de données.                                                                                                                                                                | `default`        |
| `query_parameters`           | Liste de paires clé-valeur `param1=value1[,&param2=value2], ...`. Pour certains paramètres, aucune valeur n’est requise. Les noms de paramètres et les valeurs sont sensibles à la casse. | -                |

<div id="connection-string-notes">
  ### Remarques
</div>

Si le nom d’utilisateur, le mot de passe ou la base de données sont indiqués dans la chaîne de connexion, ils ne peuvent pas l’être avec `--user`, `--password` ou `--database` (et inversement).

La partie hôte peut être soit un nom d’hôte, soit une adresse IPv4 ou IPv6.
Les adresses IPv6 doivent être placées entre `[]` :

```text
clickhouse://[2001:db8::1234]
```

Les chaînes de connexion peuvent contenir plusieurs hôtes.
Le client ClickHouse essaiera de se connecter à ces hôtes dans l&#39;ordre (de gauche à droite).
Une fois la connexion établie, il ne tentera pas de se connecter aux hôtes restants.

La chaîne de connexion doit être spécifiée en premier argument de `clickHouse-client`.
La chaîne de connexion peut être combinée avec un nombre arbitraire d&#39;autres [options de ligne de commande](#command-line-options), à l&#39;exception de `--host` et `--port`.

Les clés suivantes sont autorisées pour `query_parameters` :

| Clé               | Description                                                                                                                                                                  |
| ----------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `secure` (ou `s`) | Si elle est spécifiée, le client se connectera au serveur via une connexion sécurisée (TLS). Voir `--secure` dans les [options de ligne de commande](#command-line-options). |

**Encodage en pourcentage**

Les caractères non ASCII, les espaces et les caractères spéciaux dans les paramètres suivants doivent être [encodés en pourcentage](https://en.wikipedia.org/wiki/URL_encoding) :

* `user`
* `password`
* `hosts`
* `database`
* `query parameters`

<div id="cli-queries-with-parameters-examples">
  ### Exemples
</div>

Connectez-vous à `localhost` sur le port 9000 et exécutez la requête `SELECT 1`.

```bash
clickhouse-client clickhouse://localhost:9000 --query "SELECT 1"
```

Connectez-vous à `localhost` en tant qu’utilisateur `john` avec le mot de passe `secret`, l’hôte `127.0.0.1` et le port `9000`

```bash
clickhouse-client clickhouse://john:secret@127.0.0.1:9000
```

Connectez-vous à `localhost` avec l’utilisateur `default`, l’hôte ayant pour adresse IPv6 `[::1]` et le port `9000`.

```bash
clickhouse-client clickhouse://[::1]:9000
```

Connectez-vous à `localhost` sur le port 9000 en mode multiligne.

```bash
clickhouse-client clickhouse://localhost:9000 '-m'
```

Connectez-vous à `localhost` sur le port 9000 avec l’utilisateur `default`.

```bash
clickhouse-client clickhouse://default@localhost:9000

# equivalent to:
clickhouse-client clickhouse://localhost:9000 --user default
```

Connectez-vous à `localhost` sur le port 9000 et utilisez la base de données `my_database` par défaut.

```bash
clickhouse-client clickhouse://localhost:9000/my_database

# equivalent to:
clickhouse-client clickhouse://localhost:9000 --database my_database
```

Connectez-vous à `localhost` sur le port 9000, utilisez par défaut la base de données `my_database` spécifiée dans la chaîne de connexion et activez une connexion sécurisée à l’aide du paramètre abrégé `s`.

```bash
clickhouse-client clickhouse://localhost/my_database?s

# equivalent to:
clickhouse-client clickhouse://localhost/my_database -s
```

Connectez-vous à l’hôte par défaut en utilisant le port par défaut, l’utilisateur `default` et la base de données `default`.

```bash
clickhouse-client clickhouse:
```

Connectez-vous à l’hôte par défaut sur le port par défaut, avec l’utilisateur `my_user` et sans mot de passe.

```bash
clickhouse-client clickhouse://my_user@

# Using a blank password between : and @ means to asking the user to enter the password before starting the connection.
clickhouse-client clickhouse://my_user:@
```

Connectez-vous à `localhost` en utilisant l’adresse e-mail comme nom d’utilisateur. Le symbole `@` est encodé en pourcentage sous la forme `%40`.

```bash
clickhouse-client clickhouse://some_user%40some_mail.com@localhost:9000
```

Connectez-vous à l’un de ces deux hôtes : `192.168.1.15`, `192.168.1.25`.

```bash
clickhouse-client clickhouse://192.168.1.15,192.168.1.25
```

<div id="query-id-format">
  ## Format de l’ID de requête
</div>

En mode interactif, ClickHouse Client affiche l’ID de chaque requête. Par défaut, cet ID est présenté comme suit :

```sql
Query id: 927f137d-00f1-4175-8914-0dd066365e96
```

Un format personnalisé peut être défini dans un fichier de configuration, à l’intérieur d’une balise `query_id_formats`. Le placeholder `{query_id}` dans la chaîne de format est remplacé par l’identifiant de la requête. Plusieurs chaînes de format sont autorisées dans la balise.
Cette fonctionnalité peut être utilisée pour générer des URL facilitant le profilage des requêtes.

**Exemple**

```xml
<config>
  <query_id_formats>
    <speedscope>http://speedscope-host/#profileURL=qp%3Fid%3D{query_id}</speedscope>
  </query_id_formats>
</config>
```

Avec la configuration ci-dessus, l&#39;ID d&#39;une requête s&#39;affiche au format suivant :

```response
speedscope:http://speedscope-host/#profileURL=qp%3Fid%3Dc8ecc783-e753-4b38-97f1-42cddfb98b7d
```

<div id="configuration_files">
  ## Fichiers de configuration
</div>

Le ClickHouse Client utilise le premier fichier existant parmi les suivants :

* Un fichier défini avec le paramètre `-c [ -C, --config, --config-file ]`.
* `./clickhouse-client.[xml|yaml|yml]`
* `$XDG_CONFIG_HOME/clickhouse/config.[xml|yaml|yml]` (ou `~/.config/clickhouse/config.[xml|yaml|yml]` si `XDG_CONFIG_HOME` n&#39;est pas défini)
* `~/.clickhouse-client/config.[xml|yaml|yml]`
* `/etc/clickhouse-client/config.[xml|yaml|yml]`

Consultez un exemple de fichier de configuration dans le dépôt ClickHouse : [`clickhouse-client.xml`](https://github.com/ClickHouse/ClickHouse/blob/master/programs/client/clickhouse-client.xml)

<Tabs>
  <TabItem value="xml" label="XML" default>
    ```xml
    <config>
        <user>username</user>
        <password>password</password>
        <secure>true</secure>
        <openSSL>
          <client>
            <caConfig>/etc/ssl/cert.pem</caConfig>
          </client>
        </openSSL>
    </config>
    ```
  </TabItem>

  <TabItem value="yaml" label="YAML">
    ```yaml
    user: username
    password: 'password'
    secure: true
    openSSL:
      client:
        caConfig: '/etc/ssl/cert.pem'
    ```
  </TabItem>
</Tabs>

<div id="environment-variable-options">
  ## Options des variables d&#39;environnement
</div>

Le nom d&#39;utilisateur, le mot de passe et l&#39;hôte peuvent être définis au moyen des variables d&#39;environnement `CLICKHOUSE_USER`, `CLICKHOUSE_PASSWORD` et `CLICKHOUSE_HOST`.
Les arguments de ligne de commande `--user`, `--password` ou `--host`, ou une [chaîne de connexion](#connection_string) (si elle est spécifiée), ont priorité sur les variables d&#39;environnement.

<div id="command-line-options">
  ## Options de ligne de commande
</div>

Toutes les options de ligne de commande peuvent être spécifiées directement sur la ligne de commande ou définies par défaut dans le [fichier de configuration](#configuration_files).

<div id="command-line-options-general">
  ### Options générales
</div>

| Option                                              | Description                                                                                                                                                             | Par défaut                   |
| --------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------- |
| `-c [ -C, --config, --config-file ] <path-to-file>` | Emplacement du fichier de configuration du client, s’il ne se trouve pas dans l’un des emplacements par défaut. Voir [Fichiers de configuration](#configuration_files). | -                            |
| `--help`                                            | Affiche le résumé d’utilisation puis quitte. Combinez avec `--verbose` pour afficher toutes les options possibles, y compris les paramètres de requête.                 | -                            |
| `--history_file <path-to-file>`                     | Chemin vers un fichier contenant l’historique des commandes.                                                                                                            | -                            |
| `--history_max_entries`                             | Nombre maximal d’entrées dans le fichier d’historique.                                                                                                                  | `1000000` (1 million)        |
| `--prompt <prompt>`                                 | Indique une invite personnalisée.                                                                                                                                       | Le `display_name` du serveur |
| `--verbose`                                         | Augmente le niveau de détail de la sortie.                                                                                                                              | -                            |
| `-V [ --version ]`                                  | Affiche la version puis quitte.                                                                                                                                         | -                            |

<div id="command-line-options-connection">
  ### Options de connexion
</div>

| Option                               | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                             | Default                                                                                                                                            |
| ------------------------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------- |
| `--connection <name>`                | Le nom des informations de connexion préconfigurées dans le fichier de configuration. Voir [Identifiants de connexion](#connection-credentials).                                                                                                                                                                                                                                                                                                                        | -                                                                                                                                                  |
| `-d [ --database ] <database>`       | Sélectionne la base de données à utiliser par défaut pour cette connexion.                                                                                                                                                                                                                                                                                                                                                                                              | La base de données courante issue des paramètres du serveur (`default` par défaut)                                                                 |
| `-h [ --host ] <host>`               | Le nom d&#39;hôte du serveur ClickHouse auquel se connecter. Il peut s&#39;agir d&#39;un nom d&#39;hôte, d&#39;une adresse IPv4 ou d&#39;une adresse IPv6. Plusieurs hôtes peuvent être fournis à l&#39;aide de plusieurs arguments.                                                                                                                                                                                                                                    | `localhost`                                                                                                                                        |
| `--jwt <value>`                      | Utilise un JSON Web Token (JWT) pour l&#39;authentification. <br /><br />L&#39;autorisation JWT côté serveur est disponible uniquement dans ClickHouse Cloud.                                                                                                                                                                                                                                                                                                           | -                                                                                                                                                  |
| `login`                              | Lance le flux OAuth par autorisation de périphérique afin de s&#39;authentifier via un IdP. <br /><br />Pour les hôtes ClickHouse Cloud, les variables OAuth sont déduites automatiquement ; sinon, elles doivent être fournies avec `--oauth-url`, `--oauth-client-id` et `--oauth-audience`.                                                                                                                                                                          | -                                                                                                                                                  |
| `--no-warnings`                      | Désactive l&#39;affichage des avertissements provenant de `system.warnings` lorsque le client se connecte au serveur.                                                                                                                                                                                                                                                                                                                                                   | -                                                                                                                                                  |
| `--no-server-client-version-message` | Supprime le message d&#39;incompatibilité de version entre le serveur et le client lorsque le client se connecte au serveur.                                                                                                                                                                                                                                                                                                                                            | -                                                                                                                                                  |
| `--password <password>`              | Le mot de passe de l&#39;utilisateur de base de données. Vous pouvez également spécifier le mot de passe d&#39;une connexion dans le fichier de configuration. Si vous ne spécifiez pas le mot de passe, le client vous le demandera.                                                                                                                                                                                                                                   | -                                                                                                                                                  |
| `--port <port>`                      | Le port sur lequel le serveur accepte les connexions. Les ports par défaut sont 9440 (TLS) et 9000 (sans TLS). <br /><br />Remarque : le client utilise le protocole natif, et non HTTP(S).                                                                                                                                                                                                                                                                             | `9440` si `--secure` est spécifié, `9000` sinon. La valeur par défaut est toujours `9440` si le nom d&#39;hôte se termine par `.clickhouse.cloud`. |
| `-s [ --secure ]`                    | Indique s&#39;il faut utiliser TLS. <br /><br />Activé automatiquement lors d&#39;une connexion au port 9440 (le port sécurisé par défaut) ou à ClickHouse Cloud. <br /><br />Vous devrez peut-être configurer vos certificats CA dans le [fichier de configuration](#configuration_files). Les paramètres de configuration disponibles sont les mêmes que pour la [configuration TLS côté serveur](../operations/server-configuration-parameters/settings.md#openssl). | Activé automatiquement lors d&#39;une connexion au port 9440 ou à ClickHouse Cloud                                                                 |
| `--ssh-key-file <path-to-file>`      | Fichier contenant la clé privée SSH permettant de s&#39;authentifier auprès du serveur.                                                                                                                                                                                                                                                                                                                                                                                 | -                                                                                                                                                  |
| `--ssh-key-passphrase <value>`       | Phrase de passe de la clé privée SSH spécifiée dans `--ssh-key-file`.                                                                                                                                                                                                                                                                                                                                                                                                   | -                                                                                                                                                  |
| `--tls-sni-override <server name>`   | Si TLS est utilisé, le nom du serveur (SNI) à transmettre lors de la négociation.                                                                                                                                                                                                                                                                                                                                                                                       | L&#39;hôte fourni via `-h` ou `--host`.                                                                                                            |
| `-u [ --user ] <username>`           | L&#39;utilisateur de base de données avec lequel se connecter.                                                                                                                                                                                                                                                                                                                                                                                                          | `default`                                                                                                                                          |

:::note
Au lieu des options `--host`, `--port`, `--user` et `--password`, le client prend également en charge les [chaînes de connexion](#connection_string).
:::

<div id="command-line-options-query">
  ### Options de requête
</div>

| Option                          | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| ------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `--param_<name>=<value>`        | Valeur de substitution d’un paramètre pour une [requête avec paramètres](#cli-queries-with-parameters).                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| `-q [ --query ] <query>`        | Requête à exécuter en mode batch. Peut être spécifiée plusieurs fois (`--query "SELECT 1" --query "SELECT 2"`) ou une seule fois avec plusieurs requêtes séparées par des points-virgules (`--query "SELECT 1; SELECT 2;"`). Dans ce dernier cas, les requêtes `INSERT` utilisant des formats autres que `VALUES` doivent être séparées par des lignes vides. <br /><br />Il est également possible de spécifier une seule requête sans paramètre : `clickhouse-client "SELECT 1"` <br /><br />Ne peut pas être utilisé avec `--queries-file`.                                                                                                                                                                                     |
| `--queries-file <path-to-file>` | Chemin d’accès vers un fichier contenant des requêtes. `--queries-file` peut être spécifié plusieurs fois, par exemple `--queries-file queries1.sql --queries-file queries2.sql`. <br /><br />Ne peut pas être utilisé avec `--query`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| `-m [ --multiline ]`            | Si cette option est spécifiée, autorise les requêtes sur plusieurs lignes (la requête n’est pas envoyée en appuyant sur Entrée). Les requêtes ne sont envoyées que lorsqu’elles se terminent par un point-virgule.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| `--inline-insert-data`          | Envoie `INSERT ... VALUES` (et les autres formats intégrés) tels quels dans le texte de la requête, au lieu de convertir les données en blocs au format natif. Le serveur analyse lui-même les données intégrées, ce qui évite l’aller-retour consistant à renvoyer au client la structure de la table et les valeurs par défaut des colonnes. Cela peut améliorer les performances pour un grand nombre de petites insertions via le protocole natif. Définit automatiquement [`send_table_structure_on_insert_with_inline_data`](/fr/operations/settings/settings#send_table_structure_on_insert_with_inline_data) sur `0`. Ne peut pas être combiné avec des données intégrées et des données externes (depuis stdin ou `INFILE`). |

<div id="command-line-options-query-settings">
  ### Paramètres de requête
</div>

Les paramètres de requête peuvent être indiqués comme options de ligne de commande du client, par exemple :

```bash
$ clickhouse-client --max_threads 1
```

Voir [Paramètres](../operations/settings/settings.md) pour la liste des paramètres.

<div id="command-line-options-formatting">
  ### Options de formatage
</div>

| Option                            | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    | Par défaut                                                        |
| --------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------- |
| `-f [ --format ] <format>`        | Utilise le format spécifié pour afficher le résultat. <br /><br />Voir [Formats des données d’entrée et de sortie](formats.md) pour obtenir la liste des formats pris en charge.                                                                                                                                                                                                                                                                                                                                                                                                                                                               | `TabSeparated`                                                    |
| `--pager <command>`               | Redirige toute la sortie vers cette commande. Généralement `less` (par ex. `less -S` pour afficher des jeux de résultats larges) ou une commande similaire.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    | -                                                                 |
| `-E [ --vertical ]`               | Utilise le [format vertical](/fr/interfaces/formats/Vertical) pour afficher le résultat. Cela revient à `–-format Vertical`. Dans ce format, chaque valeur est affichée sur une ligne distincte, ce qui est utile pour afficher des tables larges.                                                                                                                                                                                                                                                                                                                                                                                                | -                                                                 |
| `--echo [ <bool> ]`               | Affiche chaque requête avant son exécution. Accepte une valeur booléenne facultative.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          | `true` en mode interactif, `false` en mode non interactif (batch) |
| `--echo-formatted [ <bool> ]`     | Met en forme les requêtes affichées par écho. Accepte une valeur booléenne facultative.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        | `true` en mode interactif, `false` en mode non interactif (batch) |
| `--echo-query-id [ <bool> ]`      | Affiche l’ID de requête avant l’exécution. Accepte une valeur booléenne facultative.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           | `true` en mode interactif, `false` en mode non interactif (batch) |
| `--echo-query-separator <string>` | Affiche ce séparateur avant la requête mise en forme affichée par écho (nécessite `--echo-formatted`), ce qui permet de distinguer plus facilement la requête saisie de sa version reformatée affichée par écho.                                                                                                                                                                                                                                                                                                                                                                                                                               | Vide (désactivé)                                                  |
| `--highlight [ --hilite ] <bool>` | Active ou désactive la coloration syntaxique de l’invite de commande et des requêtes affichées par écho.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       | `true`                                                            |
| `--hints <bool>`                  | Affiche, pendant la saisie, des indications d’autocomplétion (texte « fantôme » intégré) pour la suggestion la plus pertinente lorsque le curseur se trouve à la fin de l’entrée. Parcourez les indications avec Haut/Bas (ou Ctrl-Haut/Ctrl-Bas) ; acceptez l’indication intégrée avec Tab ou Droite ; `Enter` n’accepte une indication qu’après qu’elle a été explicitement sélectionnée et, sinon, exécute la requête ; `Tab` ouvre également la liste classique de complétion. Nécessite `--highlight` (les indications ont besoin de couleur) ainsi que le mécanisme de suggestion (donc `--disable_suggestion` les désactive également). | `true`                                                            |

<div id="command-line-options-execution-details">
  ### Détails d’exécution
</div>

| Option                           | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                    | Par défaut                                                     |
| -------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | -------------------------------------------------------------- |
| `--chime [N]`                    | Écrit le caractère de contrôle `BEL` dans `stderr` lorsqu’une requête se termine (qu’elle réussisse ou échoue) après avoir duré au moins `N` secondes. N’est émis que lorsque `stderr` est attaché à un terminal (TTY) ; la redirection de `stderr` (par ex. `2>err.log`) le supprime, tandis que la redirection de `stdout` (par ex. `> result.tsv`) ne le supprime pas. Passer `--chime` sans valeur utilise le seuil par défaut. Définissez `--chime 0` pour le désactiver. | `5` secondes                                                   |
| `--enable-progress-table-toggle` | Active l’affichage/masquage du tableau de progression à l’aide de la touche de contrôle (Espace). S’applique uniquement en mode interactif lorsque l’affichage du tableau de progression est activé.                                                                                                                                                                                                                                                                           | `activé`                                                       |
| `--hardware-utilization`         | Affiche les informations d’utilisation matérielle dans la barre de progression.                                                                                                                                                                                                                                                                                                                                                                                                | -                                                              |
| `--memory-usage`                 | Si spécifié, affiche l’utilisation de la mémoire dans `stderr` en mode non interactif. <br /><br />Valeurs possibles : <br />• `none` - ne pas afficher l’utilisation de la mémoire <br />• `default` - afficher le nombre d’octets <br />• `readable` - afficher l’utilisation de la mémoire dans un format lisible                                                                                                                                                           | -                                                              |
| `--print-profile-events`         | Affiche les paquets `ProfileEvents`.                                                                                                                                                                                                                                                                                                                                                                                                                                           | -                                                              |
| `--progress`                     | Affiche la progression de l’exécution de la requête. <br /><br />Valeurs possibles : <br />• `tty\|on\|1\|true\|yes` - sortie vers le terminal en mode interactif <br />• `err` - sortie vers `stderr` en mode non interactif <br />• `off\|0\|false\|no` - désactive l’affichage de la progression                                                                                                                                                                            | `tty` en mode interactif, `off` en mode non interactif (batch) |
| `--progress-table`               | Affiche un tableau de progression avec des métriques mises à jour pendant l’exécution de la requête. <br /><br />Valeurs possibles : <br />• `tty\|on\|1\|true\|yes` - sortie vers le terminal en mode interactif <br />• `err` - sortie vers `stderr` en mode non interactif <br />• `off\|0\|false\|no` - désactive le tableau de progression                                                                                                                                | `tty` en mode interactif, `off` en mode non interactif (batch) |
| `--stacktrace`                   | Affiche les traces de pile des exceptions.                                                                                                                                                                                                                                                                                                                                                                                                                                     | -                                                              |
| `-t [ --time ]`                  | Affiche le temps d’exécution de la requête dans `stderr` en mode non interactif (pour les benchmarks).                                                                                                                                                                                                                                                                                                                                                                         | -                                                              |