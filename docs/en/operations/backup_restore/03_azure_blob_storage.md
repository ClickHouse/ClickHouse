---
description: 'Details backup/restore to or from an Azure Blob Storage endpoint'
sidebar_label: 'AzureBlobStorage'
slug: /operations/backup/azure
title: 'Backup and restore to/from Azure Blob Storage'
---

import Syntax from '@site/docs/operations/backup_restore/_snippets/_syntax.md';

# BACKUP/RESTORE to or from Azure Blob Storage {#backup-to-azure-blob-storage}

## Syntax {#syntax}

<Syntax/>

## Configuring BACKUP / RESTORE to use an AzureBlobStorage endpoint {#configuring-backuprestore-to-use-an-azureblobstorage-endpoint}

To write backups to an AzureBlobStorage container you need the following pieces of information:
- AzureBlobStorage endpoint connection string / url,
- Container,
- Path,
- Account name (if url is specified)
- Account Key (if url is specified)

The destination for a backup will be specified as:

```sql
AzureBlobStorage('<connection string>/<url>', '<container>', '<path>', '<account name>', '<account key>')
```

```sql
BACKUP TABLE data TO AzureBlobStorage('DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://azurite1:10000/devstoreaccount1/;',
    'testcontainer', 'data_backup');
RESTORE TABLE data AS data_restored FROM AzureBlobStorage('DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://azurite1:10000/devstoreaccount1/;',
    'testcontainer', 'data_backup');
```
