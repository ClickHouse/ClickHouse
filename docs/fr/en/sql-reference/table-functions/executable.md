---
description: 'La fonction de table `executable` crée une table à partir de la sortie
  d''une fonction définie par l''utilisateur (UDF) que vous définissez dans un script qui envoie des lignes vers
  **stdout**.'
keywords: ['udf', 'user defined function', 'clickhouse', 'executable', 'table', 'function']
sidebar_label: 'executable'
sidebar_position: 50
slug: /engines/table-functions/executable
title: 'executable'
doc_type: 'reference'
---

La fonction de table `executable` crée une table à partir de la sortie d&#39;une fonction définie par l&#39;utilisateur (UDF) que vous définissez dans un script qui envoie des lignes vers **stdout**. Le script exécutable est stocké dans le répertoire `users_scripts` et peut lire des données depuis n&#39;importe quelle source. Assurez-vous que votre serveur ClickHouse dispose de tous les paquets nécessaires pour exécuter le script. Par exemple, s&#39;il s&#39;agit d&#39;un script Python, assurez-vous que les paquets Python nécessaires sont installés sur le serveur.

Vous pouvez éventuellement inclure une ou plusieurs requêtes d&#39;entrée dont les résultats sont transmis à **stdin** pour être lus par le script.

:::note
L&#39;un des principaux avantages de la fonction de table `executable` et du moteur de table `Executable` par rapport aux fonctions UDF ordinaires est que ces dernières ne peuvent pas modifier le nombre de lignes. Par exemple, si l&#39;entrée contient 100 lignes, le résultat doit également en renvoyer 100. Lorsque vous utilisez la fonction de table `executable` ou le moteur de table `Executable`, votre script peut effectuer toutes les transformations de données souhaitées, y compris des agrégations complexes.
:::

<div id="syntax">
  ## Syntaxe
</div>

La fonction de table `executable` nécessite trois paramètres et accepte une liste facultative de requêtes d’entrée :

```sql
executable(script_name, format, structure, [input_query...] [,SETTINGS ...])
```

* `script_name` : le nom du fichier du script, enregistré dans le dossier `user_scripts` (le dossier par défaut du paramètre `user_scripts_path`)
* `format` : le format de la table générée
* `structure` : le schéma de la table générée
* `input_query` : une requête facultative (ou une collection, ou plusieurs requêtes) dont les résultats sont transmis au script via **stdin**

:::note
Si vous prévoyez d&#39;exécuter plusieurs fois le même script avec les mêmes requêtes d&#39;entrée, envisagez d&#39;utiliser le moteur de table [`Executable`](../../engines/table-engines/special/executable.md).
:::

Le script Python suivant s&#39;appelle `generate_random.py` et est enregistré dans le dossier `user_scripts`. Il lit un nombre `i` et affiche `i` chaînes aléatoires, chacune précédée d&#39;un nombre séparé par une tabulation :

```python
#!/usr/local/bin/python3.9

import sys
import string
import random

def main():

    # Read input value
    for number in sys.stdin:
        i = int(number)

        # Generate some random rows
        for id in range(0, i):
            letters = string.ascii_letters
            random_string =  ''.join(random.choices(letters ,k=10))
            print(str(id) + '\t' + random_string + '\n', end='')

        # Flush results to stdout
        sys.stdout.flush()

if __name__ == "__main__":
    main()
```

Exécutons le script et faisons-lui générer 10 chaînes aléatoires :

```sql
SELECT * FROM executable('generate_random.py', TabSeparated, 'id UInt32, random String', (SELECT 10))
```

La réponse ressemble à ceci :

```response
┌─id─┬─random─────┐
│  0 │ xheXXCiSkH │
│  1 │ AqxvHAoTrl │
│  2 │ JYvPCEbIkY │
│  3 │ sWgnqJwGRm │
│  4 │ fTZGrjcLon │
│  5 │ ZQINGktPnd │
│  6 │ YFSvGGoezb │
│  7 │ QyMJJZOOia │
│  8 │ NfiyDDhmcI │
│  9 │ REJRdJpWrg │
└────┴────────────┘
```

<div id="settings">
  ## Paramètres
</div>

* `send_chunk_header` - détermine s’il faut envoyer le nombre de lignes avant d’envoyer un chunk de données à traiter. La valeur par défaut est `false`.
* `pool_size` — Taille du pool. Si `pool_size` est défini sur 0, aucune restriction de taille du pool n’est appliquée. La valeur par défaut est `16`.
* `max_command_execution_time` — temps d’exécution maximal de la commande du script exécutable pour traiter un bloc de données. Exprimé en secondes. La valeur par défaut est 10.
* `command_termination_timeout` — le script exécutable doit contenir une boucle principale de lecture-écriture. Une fois la fonction de table détruite, le pipe est fermé et l’exécutable dispose de `command_termination_timeout` secondes pour s’arrêter avant que ClickHouse n’envoie le signal SIGTERM au processus enfant. Exprimé en secondes. La valeur par défaut est 10.
* `command_read_timeout` - délai d’expiration pour la lecture des données depuis le stdout de la commande, en millisecondes. La valeur par défaut est 10000.
* `command_write_timeout` - délai d’expiration pour l’écriture des données vers le stdin de la commande, en millisecondes. La valeur par défaut est 10000.

<div id="passing-query-results-to-a-script">
  ## Transmettre le résultat de la requête à un script
</div>

Consultez également l’exemple du moteur de table `Executable` sur [la manière de transmettre le résultat de la requête à un script](../../engines/table-engines/special/executable.md#passing-query-results-to-a-script). Voici comment exécuter le même script que dans cet exemple à l’aide de la fonction de table `executable` :

```sql
SELECT * FROM executable(
    'sentiment.py',
    TabSeparated,
    'id UInt64, sentiment Float32',
    (SELECT id, comment FROM hackernews WHERE id > 0 AND comment != '' LIMIT 20)
);
```