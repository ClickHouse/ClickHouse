---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 39
toc_title: "\u0130\xE7 S\xF6z Dictionariesl\xFCkler"
---

# İç Söz Dictionarieslükler {#internal_dicts}

ClickHouse, bir geobase ile çalışmak için yerleşik bir özellik içerir.

Bu size sağlar:

-   Adını istediğiniz dilde almak için bölgenin kimliğini kullanın.
-   Bir şehir, bölge, federal bölge, ülke veya kıtanın kimliğini almak için bölgenin kimliğini kullanın.
-   Bir bölgenin başka bir bölgenin parçası olup olmadığını kontrol edin.
-   Ana bölgeler zinciri alın.

Tüm fonksiyonları destek “translocality,” aynı anda bölge mülkiyeti farklı bakış açıları kullanma yeteneği. Daha fazla bilgi için bölüme bakın “Functions for working with Yandex.Metrica dictionaries”.

İç sözlükler varsayılan pakette devre dışı bırakılır.
Bunları etkinleştirmek için, parametreleri uncomment `path_to_regions_hierarchy_file` ve `path_to_regions_names_files` sunucu yapılandırma dosyasında.

Geobase metin dosyalarından yüklenir.

Place the `regions_hierarchy*.txt` dosyaları içine `path_to_regions_hierarchy_file` dizin. Bu yapılandırma parametresi, `regions_hierarchy.txt` dosya (varsayılan bölgesel hiyerarşi) ve diğer dosyalar (`regions_hierarchy_ua.txt`) aynı dizinde bulunmalıdır.

Koy... `regions_names_*.txt` dosyalar içinde `path_to_regions_names_files` dizin.

Bu dosyaları kendiniz de oluşturabilirsiniz. Dosya biçimi aşağıdaki gibidir:

`regions_hierarchy*.txt`: TabSeparated (başlık yok), sütunlar:

-   bölge kimliği (`UInt32`)
-   üst bölge kimliği (`UInt32`)
-   bölge tipi (`UInt8`): 1 - kıta, 3-ülke, 4-federal bölge, 5-bölge, 6-şehir; diğer türlerin değerleri yoktur
-   nüfuslu (`UInt32`) — optional column

`regions_names_*.txt`: TabSeparated (başlık yok), sütunlar:

-   bölge kimliği (`UInt32`)
-   bölge adı (`String`) — Can't contain tabs or line feeds, even escaped ones.

RAM'de depolamak için düz bir dizi kullanılır. Bu nedenle, IDs bir milyondan fazla olmamalıdır.

Sözlükler sunucuyu yeniden başlatmadan güncellenebilir. Ancak, kullanılabilir sözlükler kümesi güncelleştirilmez.
Güncellemeler için dosya değiştirme süreleri kontrol edilir. Bir dosya değiştiyse, sözlük güncelleştirilir.
Değişiklikleri kontrol etmek için Aralık `builtin_dictionaries_reload_interval` parametre.
Sözlük güncelleştirmeleri (ilk kullanımda yükleme dışında) sorguları engellemez. Güncelleştirmeler sırasında, sorgular sözlüklerin eski sürümlerini kullanır. Güncelleştirme sırasında bir hata oluşursa, hata sunucu günlüğüne yazılır ve sorgular sözlüklerin eski sürümünü kullanmaya devam eder.

Sözlükleri geobase ile periyodik olarak güncellemenizi öneririz. Bir güncelleme sırasında yeni dosyalar oluşturun ve bunları ayrı bir konuma yazın. Her şey hazır olduğunda, bunları sunucu tarafından kullanılan dosyalara yeniden adlandırın.

OS tanımlayıcıları ve Yandex ile çalışmak için işlevler de vardır.Metrica arama motorları, ancak kullanılmamalıdır.

[Orijinal makale](https://clickhouse.tech/docs/en/query_language/dicts/internal_dicts/) <!--hide-->
