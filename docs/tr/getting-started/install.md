---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 11
toc_title: Kurulum
---

# Kurulum {#installation}

## Sistem Gereksinimleri {#system-requirements}

ClickHouse, x86_64, AArch64 veya PowerPC64LE CPU mimarisine sahip herhangi bir Linux, FreeBSD veya Mac OS X üzerinde çalışabilir.

Resmi önceden oluşturulmuş ikili dosyalar genellikle x86_64 ve kaldıraç sse 4.2 komut seti için derlenir, bu nedenle destekleyen CPU'nun aksi belirtilmedikçe ek bir sistem gereksinimi haline gelir. Geçerli CPU'nun sse 4.2 desteği olup olmadığını kontrol etmek için komut:

``` bash
$ grep -q sse4_2 /proc/cpuinfo && echo "SSE 4.2 supported" || echo "SSE 4.2 not supported"
```

SSE 4.2'yi desteklemeyen veya AArch64 veya PowerPC64LE mimarisine sahip işlemcilerde Clickhouse'u çalıştırmak için şunları yapmalısınız [kaynaklardan ClickHouse oluşturun](#from-sources) uygun yapılandırma ayarlamaları ile.

## Mevcut Kurulum Seçenekleri {#available-installation-options}

### DEB paket fromlerinden {#install-from-deb-packages}

Resmi önceden derlenmiş kullanılması tavsiye edilir `deb` Debian veya Ubuntu için paketler. Paketleri yüklemek için bu komutları çalıştırın:

``` bash
{% include 'install/deb.sh' %}
```

En son sürümü kullanmak istiyorsanız, değiştirin `stable` ile `testing` (bu, test ortamlarınız için önerilir).

Ayrıca paketleri manuel olarak indirebilir ve yükleyebilirsiniz [burada](https://repo.clickhouse.tech/deb/stable/main/).

#### Paketler {#packages}

-   `clickhouse-common-static` — Installs ClickHouse compiled binary files.
-   `clickhouse-server` — Creates a symbolic link for `clickhouse-server` ve varsayılan sunucu yapılandırmasını yükler.
-   `clickhouse-client` — Creates a symbolic link for `clickhouse-client` ve diğer istemci ile ilgili araçlar. ve istemci yapılandırma dosyalarını yükler.
-   `clickhouse-common-static-dbg` — Installs ClickHouse compiled binary files with debug info.

### RPM paket fromlerinden {#from-rpm-packages}

Resmi önceden derlenmiş kullanılması tavsiye edilir `rpm` CentOS, RedHat ve diğer tüm rpm tabanlı Linux dağıtımları için paketler.

İlk olarak, resmi depoyu eklemeniz gerekir:

``` bash
sudo yum install yum-utils
sudo rpm --import https://repo.clickhouse.tech/CLICKHOUSE-KEY.GPG
sudo yum-config-manager --add-repo https://repo.clickhouse.tech/rpm/stable/x86_64
```

En son sürümü kullanmak istiyorsanız, değiştirin `stable` ile `testing` (bu, test ortamlarınız için önerilir). Bu `prestable` etiket de bazen kullanılabilir.

Sonra paketleri yüklemek için bu komutları çalıştırın:

``` bash
sudo yum install clickhouse-server clickhouse-client
```

Ayrıca paketleri manuel olarak indirebilir ve yükleyebilirsiniz [burada](https://repo.clickhouse.tech/rpm/stable/x86_64).

### Tgz Arşivlerinden {#from-tgz-archives}

Resmi önceden derlenmiş kullanılması tavsiye edilir `tgz` Arch ,iv ,es for tüm Linux dağıtım installationları, kurulumu `deb` veya `rpm` paketler mümkün değildir.

Gerekli sürümü ile indirilebilir `curl` veya `wget` depo fromdan https://repo.clickhouse.tech/tgz/.
Bundan sonra indirilen arşivler açılmalı ve kurulum komut dosyaları ile kurulmalıdır. En son sürüm için örnek:

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

Üretim ortamları için en son teknolojiyi kullanmanız önerilir `stable`-sürüm. Numarasını GitHub sayfasında bulabilirsiniz https://github.com/ClickHouse/ClickHouse/tags postfix ile `-stable`.

### Docker Görüntüden {#from-docker-image}

Docker içinde ClickHouse çalıştırmak için kılavuzu izleyin [Docker Hub](https://hub.docker.com/r/yandex/clickhouse-server/). Bu görüntüler resmi `deb` paketler içinde.

### Kaynaklardan {#from-sources}

Clickhouse'u el ile derlemek için aşağıdaki talimatları izleyin [Linux](../development/build.md) veya [Mac OS X](../development/build-osx.md).

Paketleri derleyebilir ve yükleyebilir veya paketleri yüklemeden programları kullanabilirsiniz. Ayrıca elle inşa ederek SSE 4.2 gereksinimini devre dışı bırakabilir veya AArch64 CPU'lar için oluşturabilirsiniz.

      Client: programs/clickhouse-client
      Server: programs/clickhouse-server

Bir veri ve meta veri klasörleri oluşturmanız gerekir ve `chown` onları istenen kullanıcı için. Yolları sunucu yapılandırmasında değiştirilebilir (src / programlar / sunucu / config.xml), varsayılan olarak:

      /opt/clickhouse/data/default/
      /opt/clickhouse/metadata/default/

Gentoo üzerinde, sadece kullanabilirsiniz `emerge clickhouse` Clickhouse'u kaynaklardan yüklemek için.

## Başlatmak {#launch}

Sunucuyu bir daemon olarak başlatmak için çalıştırın:

``` bash
$ sudo service clickhouse-server start
```

Yok eğer `service` command, run as

``` bash
$ sudo /etc/init.d/clickhouse-server start
```

Günlükleri görmek `/var/log/clickhouse-server/` dizin.

Sunucu başlatılmazsa, dosyadaki yapılandırmaları kontrol edin `/etc/clickhouse-server/config.xml`.

Ayrıca sunucuyu konsoldan manuel olarak başlatabilirsiniz:

``` bash
$ clickhouse-server --config-file=/etc/clickhouse-server/config.xml
```

Bu durumda, günlük geliştirme sırasında uygun olan konsola yazdırılacaktır.
Yapılandırma dosyası geçerli dizinde ise, `--config-file` parametre. Varsayılan olarak, kullanır `./config.xml`.

ClickHouse erişim kısıtlama ayarlarını destekler. Bulun theurlar. `users.xml` dosya (yanındaki `config.xml`).
Varsayılan olarak, erişim için herhangi bir yerden izin verilir `default` Kullanıcı, şifre olmadan. Görmek `user/default/networks`.
Daha fazla bilgi için bölüme bakın [“Configuration Files”](../operations/configuration-files.md).

Sunucuyu başlattıktan sonra, ona bağlanmak için komut satırı istemcisini kullanabilirsiniz:

``` bash
$ clickhouse-client
```

Varsayılan olarak, bağlanır `localhost:9000` kullanıcı adına `default` şifre olmadan. Kullanarak uzak bir sunucuya bağlanmak için de kullanılabilir `--host` tartışma.

Terminal UTF-8 kodlamasını kullanmalıdır.
Daha fazla bilgi için bölüme bakın [“Command-line client”](../interfaces/cli.md).

Örnek:

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

**Tebrikler, sistem çalışıyor!**

Denemeye devam etmek için, test veri kümelerinden birini indirebilir veya şunları yapabilirsiniz [öğretici](https://clickhouse.tech/tutorial.html).

[Orijinal makale](https://clickhouse.tech/docs/en/getting_started/install/) <!--hide-->
