---
description: 'يتيح معالجة الملفات من Azure Blob Storage بالتوازي باستخدام العديد من
  العُقد في عنقود محدد.'
sidebar_label: 'azureBlobStorageCluster'
sidebar_position: 15
slug: /sql-reference/table-functions/azureBlobStorageCluster
title: 'azureBlobStorageCluster'
doc_type: 'مرجع'
---

يتيح معالجة الملفات من [Azure Blob Storage](https://azure.microsoft.com/en-us/products/storage/blobs) بالتوازي باستخدام العديد من العُقد في عنقود محدد. على العقدة البادئة، ينشئ اتصالًا بجميع العُقد في العنقود، ويوسّع علامات النجمة في مسار ملف S3، ثم يوزّع كل ملف ديناميكيًا. وعلى العقدة العاملة، يطلب من العقدة البادئة المهمة التالية المطلوب معالجتها ثم يعالجها. ويتكرر ذلك حتى تكتمل جميع المهام.
تشبه دالة الجدول هذه الدالة [s3Cluster](../../sql-reference/table-functions/s3Cluster.md).

<div id="syntax">
  ## الصيغة
</div>

```sql
azureBlobStorageCluster(cluster_name, connection_string|storage_account_url, container_name, blobpath, [account_name, account_key, format, compression, structure])
```

<div id="arguments">
  ## الوسائط
</div>

| Argument            | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| ------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `cluster_name`      | اسم العنقود المُستخدَم لبناء مجموعة من العناوين ومعلمات الاتصال بالخوادم البعيدة والمحلية.                                                                                                                                                                                                                                                                                                                                                                                                        |
| `connection_string` | storage&#95;account&#95;url&#96; — تتضمن سلسلة الاتصال اسم الحساب والمفتاح ([إنشاء سلسلة اتصال](https://learn.microsoft.com/en-us/azure/storage/common/storage-configure-connection-string?toc=%2Fazure%2Fstorage%2Fblobs%2Ftoc.json\&bc=%2Fazure%2Fstorage%2Fblobs%2Fbreadcrumb%2Ftoc.json#configure-a-connection-string-for-an-azure-storage-account))، أو يمكنك أيضًا توفير URL حساب التخزين هنا واسم الحساب ومفتاح الحساب كمعلمات منفصلة (راجع المعلمتَين account&#95;name و account&#95;key) |
| `container_name`    | اسم الحاوية                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| `blobpath`          | مسار الملف. يدعم أحرف البدل التالية في وضع `readonly`: `*`، `**`، `?`، `{abc,def}` و `{N..M}`، حيث إن `N` و `M` — أعداد، و `'abc'` و `'def'` — سلاسل نصية.                                                                                                                                                                                                                                                                                                                                        |
| `account_name`      | إذا استُخدم storage&#95;account&#95;url، فيمكن تحديد اسم الحساب هنا                                                                                                                                                                                                                                                                                                                                                                                                                               |
| `account_key`       | إذا استُخدم storage&#95;account&#95;url، فيمكن تحديد مفتاح الحساب هنا                                                                                                                                                                                                                                                                                                                                                                                                                             |
| `format`            | [تنسيق](/ar/sql-reference/formats) الملف.                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| `compression`       | القيم المدعومة: `none`، `gzip/gz`، `brotli/br`، `xz/LZMA`، `zstd/zst`. افتراضيًا، سيُكتشف الضغط تلقائيًا من امتداد الملف. (كما لو كان مضبوطًا على `auto`).                                                                                                                                                                                                                                                                                                                                        |
| `structure`         | بنية الجدول. التنسيق: `'column1_name column1_type, column2_name column2_type, ...'`.                                                                                                                                                                                                                                                                                                                                                                                                              |

<div id="returned_value">
  ## القيمة المعادة
</div>

جدول ذو البنية المحددة لقراءة البيانات من الملف المحدد أو كتابتها فيه.

<div id="examples">
  ## أمثلة
</div>

على غرار محرك الجدول [AzureBlobStorage](/ar/engines/table-engines/integrations/azureBlobStorage)، يمكن للمستخدمين استخدام محاكي Azurite لتطوير Azure Storage محليًا. تتوفر مزيد من التفاصيل [هنا](https://learn.microsoft.com/en-us/azure/storage/common/storage-use-azurite?tabs=docker-hub%2Cblob-storage). نفترض أدناه أن Azurite متاح على اسم المضيف `azurite1`.

اعرض عدد السجلات في الملف `test_cluster_*.csv` باستخدام جميع العقد في عنقود `cluster_simple`:

```sql
SELECT count(*) FROM azureBlobStorageCluster(
        'cluster_simple', 'http://azurite1:10000/devstoreaccount1', 'testcontainer', 'test_cluster_count.csv', 'devstoreaccount1',
        'Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==', 'CSV',
        'auto', 'key UInt64')
```

<div id="using-shared-access-signatures-sas-sas-tokens">
  ## استخدام تواقيع الوصول المشتركة (SAS)
</div>

راجع [azureBlobStorage](/ar/sql-reference/table-functions/azureBlobStorage#using-shared-access-signatures-sas-sas-tokens) للاطلاع على أمثلة.

<div id="related">
  ## ذات صلة
</div>

* [محرك الجدول AzureBlobStorage](../../engines/table-engines/integrations/azureBlobStorage.md)
* [دالة الجدول azureBlobStorage](../../sql-reference/table-functions/azureBlobStorage.md)