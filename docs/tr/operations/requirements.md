---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 44
toc_title: Gereksinimler
---

# Gereksinimler {#requirements}

## CPU {#cpu}

Önceden oluşturulmuş deb paketlerinden kurulum için, x86_64 mimarisine sahip bir CPU kullanın ve sse 4.2 yönergelerini destekleyin. Clickhouse'u SSE 4.2'yi desteklemeyen veya AArch64 veya PowerPC64LE mimarisine sahip işlemcilerle çalıştırmak için, kaynaklardan Clickhouse'u oluşturmanız gerekir.

ClickHouse paralel veri işleme uygular ve mevcut tüm donanım kaynaklarını kullanır. Bir işlemci seçerken, Clickhouse'un çok sayıda çekirdeğe sahip konfigürasyonlarda daha verimli çalıştığını, ancak daha az çekirdeğe ve daha yüksek bir saat hızına sahip konfigürasyonlardan daha düşük bir saat hızına sahip olduğunu göz önünde bulundurun. Örneğin, 2600 MHz'lik 16 çekirdek, 3600 MHz'lik 8 çekirdeğe tercih edilir.

Kullanılması tavsiye edilir **Turbo Bo Boostost** ve **hyper-thre -ading** teknolojiler. Tipik bir iş yükü ile performansı önemli ölçüde artırır.

## RAM {#ram}

Önemsiz olmayan sorgular gerçekleştirmek için en az 4GB RAM kullanmanızı öneririz. ClickHouse sunucusu çok daha az miktarda RAM ile çalışabilir, ancak sorguları işlemek için bellek gerektirir.

Gerekli RAM hacmi Aşağıdakilere bağlıdır:

-   Sorguların karmaşıklığı.
-   Sorgularda işlenen veri miktarı.

Gerekli RAM hacmini hesaplamak için, aşağıdakiler için geçici verilerin boyutunu tahmin etmelisiniz [GROUP BY](../sql-reference/statements/select/group-by.md#select-group-by-clause), [DISTINCT](../sql-reference/statements/select/distinct.md#select-distinct), [JOIN](../sql-reference/statements/select/join.md#select-join) ve kullandığınız diğer işlemler.

ClickHouse geçici veriler için harici bellek kullanabilirsiniz. Görmek [Harici bellekte grupla](../sql-reference/statements/select/group-by.md#select-group-by-in-external-memory) ayrıntılar için.

## Takas Dosyası {#swap-file}

Üretim ortamları için takas dosyasını devre dışı bırakın.

## Depolama Alt Sistemi {#storage-subsystem}

Clickhouse'u yüklemek için 2GB Boş disk alanına sahip olmanız gerekir.

Verileriniz için gereken depolama hacmi ayrı ayrı hesaplanmalıdır. Değerlendirme şunları içermelidir:

-   Veri hacminin tahmini.

    Verilerin bir örneğini alabilir ve ondan bir satırın ortalama boyutunu alabilirsiniz. Ardından değeri, depolamayı planladığınız satır sayısıyla çarpın.

-   Veri sıkıştırma katsayısı.

    Veri sıkıştırma katsayısını tahmin etmek için, verilerinizin bir örneğini Clickhouse'a yükleyin ve verilerin gerçek boyutunu depolanan tablonun boyutuyla karşılaştırın. Örneğin, clickstream verileri genellikle 6-10 kez sıkıştırılır.

Saklanacak verilerin son hacmini hesaplamak için, sıkıştırma katsayısını tahmini veri hacmine uygulayın. Verileri birkaç yinelemede depolamayı planlıyorsanız, tahmini birimi yinelemelerin sayısıyla çarpın.

## Ağ {#network}

Mümkünse, 10g veya daha yüksek sınıftaki ağları kullanın.

Ağ bant genişliği, büyük miktarda Ara veriyle dağıtılmış sorguları işlemek için kritik öneme sahiptir. Ayrıca, ağ hızı çoğaltma işlemlerini etkiler.

## Yazılım {#software}

ClickHouse öncelikle Linux işletim sistemleri ailesi için geliştirilmiştir. Önerilen Linux dağıtımı Ubuntu'dur. Bu `tzdata` paket sisteme kurulmalıdır.

ClickHouse diğer işletim sistemi ailelerinde de çalışabilir. Ayrıntıları görün [Başlarken](../getting-started/index.md) belgelerin bölümü.
