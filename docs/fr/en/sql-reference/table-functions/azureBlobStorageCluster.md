---
description: 'Permet de traiter des fichiers d’Azure Blob Storage en parallèle avec de
  nombreux nœuds dans un cluster spécifié.'
sidebar_label: 'azureBlobStorageCluster'
sidebar_position: 15
slug: /sql-reference/table-functions/azureBlobStorageCluster
title: 'azureBlobStorageCluster'
doc_type: 'reference'
---

Permet de traiter des fichiers d’[Azure Blob Storage](https://azure.microsoft.com/en-us/products/storage/blobs) en parallèle avec de nombreux nœuds dans un cluster spécifié. Sur le nœud initiateur, elle crée une connexion à tous les nœuds du cluster, résout les astérisques dans le chemin de fichier S3 et répartit dynamiquement chaque fichier. Sur le nœud worker, elle demande à l’initiateur la tâche suivante à traiter, puis la traite. Ce processus se répète jusqu’à ce que toutes les tâches soient terminées.
Cette fonction de table est similaire à la fonction [s3Cluster](../../sql-reference/table-functions/s3Cluster.md).

<div id="syntax">
  ## Syntaxe
</div>

```sql
azureBlobStorageCluster(cluster_name, connection_string|storage_account_url, container_name, blobpath, [account_name, account_key, format, compression, structure])
```

<div id="arguments">
  ## Arguments
</div>

| Argument            | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| ------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `cluster_name`      | Nom d’un cluster utilisé pour constituer un ensemble d’adresses et de paramètres de connexion vers des serveurs distants et locaux.                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| `connection_string` | storage&#95;account&#95;url&#96; — connection&#95;string inclut le nom du compte et la clé ([Créer une chaîne de connexion](https://learn.microsoft.com/en-us/azure/storage/common/storage-configure-connection-string?toc=%2Fazure%2Fstorage%2Fblobs%2Ftoc.json\&bc=%2Fazure%2Fstorage%2Fblobs%2Fbreadcrumb%2Ftoc.json#configure-a-connection-string-for-an-azure-storage-account)), ou vous pouvez également fournir ici l’URL du compte de stockage, ainsi que le nom du compte et la clé du compte sous forme de paramètres distincts (voir les paramètres account&#95;name et account&#95;key) |
| `container_name`    | Nom du conteneur                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| `blobpath`          | Chemin du fichier. Prend en charge les caractères génériques suivants en mode readonly : `*`, `**`, `?`, `{abc,def}` et `{N..M}`, où `N`, `M` sont des nombres et `'abc'`, `'def'` des chaînes de caractères.                                                                                                                                                                                                                                                                                                                                                                                       |
| `account_name`      | Si storage&#95;account&#95;url est utilisé, le nom du compte peut être spécifié ici                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| `account_key`       | Si storage&#95;account&#95;url est utilisé, la clé du compte peut être spécifiée ici                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| `format`            | Le [format](/fr/sql-reference/formats) du fichier.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| `compression`       | Valeurs prises en charge : `none`, `gzip/gz`, `brotli/br`, `xz/LZMA`, `zstd/zst`. Par défaut, la compression est détectée automatiquement à partir de l’extension du fichier. (équivaut au réglage `auto`)                                                                                                                                                                                                                                                                                                                                                                                          |
| `structure`         | Structure de la table. Format : `'column1_name column1_type, column2_name column2_type, ...'`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |

<div id="returned_value">
  ## Valeur renvoyée
</div>

Une table ayant la structure spécifiée pour lire ou écrire des données dans le fichier spécifié.

<div id="examples">
  ## Exemples
</div>

Comme avec le moteur de table [AzureBlobStorage](/fr/engines/table-engines/integrations/azureBlobStorage), il est possible d&#39;utiliser l&#39;émulateur Azurite pour le développement local d&#39;Azure Storage. Pour plus de détails, voir [ici](https://learn.microsoft.com/en-us/azure/storage/common/storage-use-azurite?tabs=docker-hub%2Cblob-storage). Dans la suite, nous supposons qu&#39;Azurite est disponible sous le nom d&#39;hôte `azurite1`.

Calculez le nombre d&#39;enregistrements du fichier `test_cluster_*.csv` en utilisant tous les nœuds du cluster `cluster_simple` :

```sql
SELECT count(*) FROM azureBlobStorageCluster(
        'cluster_simple', 'http://azurite1:10000/devstoreaccount1', 'testcontainer', 'test_cluster_count.csv', 'devstoreaccount1',
        'Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==', 'CSV',
        'auto', 'key UInt64')
```

<div id="using-shared-access-signatures-sas-sas-tokens">
  ## Utilisation des signatures d’accès partagé (SAS)
</div>

Voir [azureBlobStorage](/fr/sql-reference/table-functions/azureBlobStorage#using-shared-access-signatures-sas-sas-tokens) pour des exemples.

<div id="related">
  ## Voir aussi
</div>

* [Moteur AzureBlobStorage](../../engines/table-engines/integrations/azureBlobStorage.md)
* [Fonction de table azureBlobStorage](../../sql-reference/table-functions/azureBlobStorage.md)