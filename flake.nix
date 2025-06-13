{
  outputs = inputs@{ self, nixpkgs, fenix, ... }:
    let
      system = "x86_64-linux"; # Change this if needed
      pkgs = import nixpkgs { inherit system; };
      lib = pkgs.lib;

      rustNightly = fenix.packages.${system}.toolchainOf {
        channel = "nightly";
        date = "2024-12-01";
        sha256 = "sha256-hIvPOOO05JYaBR1AaIKaqI8+n9Uk4jC2OieBfzw+IuQ=";
      };

      contribInputs = lib.filterAttrs (name: _: lib.hasPrefix "contrib-" name) inputs;
      contribEntries = builtins.attrValues (
        lib.mapAttrs' (name: value:
          let
            contribName = lib.removePrefix "contrib-" name;
            contribFolder = "contrib/${contribName}";
          in
            { name = contribFolder;
              value = {
                name = contribFolder;
                path = value;
              }; }
        ) contribInputs
      );

      contribTree = pkgs.linkFarm "contrib" contribEntries;

      filteredSrc = lib.cleanSourceWith {
        filter = name: type:
          let
            rel = lib.removePrefix (toString ./. + "/") (toString name);
            noneOf = builtins.any (prefix: lib.hasPrefix prefix rel);
            baseName = baseNameOf name;
          in
            noneOf [
              "base"
              "cmake"
              "contrib"
              "programs"
              "rust"
              "src"
              "utils"
            ] || baseName == "CMakeLists.txt";
        src = lib.cleanSource ./.;
      };
      patchSrc = pkgs.runCommand "ch-patch-scripts" {  } ''
        mkdir -p $out/src/Storages/System
        mkdir -p $out/utils/list-licenses

        cp ${filteredSrc}/src/Storages/System/StorageSystemLicenses.sh $out/src/Storages/System
        cp ${filteredSrc}/utils/list-licenses/list-licenses.sh $out/utils/list-licenses

        patchShebangs $out/src
        patchShebangs $out/utils

        substituteInPlace $out/src/Storages/System/StorageSystemLicenses.sh \
          --replace-fail '$(git rev-parse --show-toplevel)' "$NIX_BUILD_TOP/source"
        substituteInPlace $out/utils/list-licenses/list-licenses.sh \
          --replace-fail '$(git rev-parse --show-toplevel)' "$NIX_BUILD_TOP/source"
      '';
    in {
      packages.x86_64-linux.default = pkgs.llvmPackages_19.stdenv.mkDerivation {
        pname = "clickhouse";
        version = if (self ? rev) then self.rev else "dev";

        src = pkgs.symlinkJoin { name = "source"; paths = [ patchSrc contribTree filteredSrc ]; };

        nativeBuildInputs = with pkgs; [
          cmake
          ninja
          llvmPackages_19.lld

          python3
          perl
          yasm
          nasm

          libgccjit

          rustNightly.rustc
          rustNightly.cargo
        ];

        cmakeFlags = [
          "-DCOMPILER_CACHE=disabled"

          "-DENABLE_TESTS=0"

          "-DENABLE_QATLIB=0"
          "-DENABLE_DELTA_KERNEL_RS=0"
        ];

        env = {
          NIX_CFLAGS_COMPILE = "-Wno-used-but-marked-unused -Wno-unused-result";
        };
      };

      devShells.default = pkgs.mkShell {
        packages = with pkgs; [
          cmake
          ninja
          clang
          lld
        ];
      };

      apps.x86_64-linux.default = {
        type = "app";
        program = "${self.packages.x86_64-linux.default}/bin/clickhouse";
      };
    };

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs";
    fenix.url = "github:nix-community/fenix";

    # Use utils/nix/update_submodule_inputs.sh
    # Everything after first contrib- line (including it) will be replaced by the script.
    contrib-jwt-cpp = { url = "github:Thalhammer/jwt-cpp/a6927cb8140858c34e05d1a954626b9849fbcdfc"; flake = false; };
    contrib-zstd = { url = "github:facebook/zstd/437081852c396eeb61edb7d47044d8cad911885e"; flake = false; };
    contrib-lz4 = { url = "github:lz4/lz4/ebb370ca83af193212df4dcbadcc5d87bc0de2f0"; flake = false; };
    contrib-librdkafka = { url = "github:ClickHouse/librdkafka/73bde76a915cf79d7fd7c49632a86857681d6a4b"; flake = false; };
    contrib-cctz = { url = "github:ClickHouse/cctz/6440590af6962a2ed22ef574c2289a54ccf165fb"; flake = false; };
    contrib-zlib-ng = { url = "github:ClickHouse/zlib-ng/a2fbeffdc30a8b0ce6d54ee31208e2688eac4c9f"; flake = false; };
    contrib-googletest = { url = "github:google/googletest/35d0c365609296fa4730d62057c487e3cfa030ff"; flake = false; };
    contrib-capnproto = { url = "github:ClickHouse/capnproto/976209a6d18074804f60d18ef99b6a809d27dadf"; flake = false; };
    contrib-re2 = { url = "github:google/re2/85dd7ad833a73095ecf3e3baea608ba051bbe2c7"; flake = false; };
    contrib-mariadb-connector-c = { url = "github:ClickHouse/mariadb-connector-c/d0a788c5b9fcaca2368d9233770d3ca91ea79f88"; flake = false; };
    contrib-jemalloc = { url = "github:jemalloc/jemalloc/41a859ef7325569c6c25f92d294d45123bb81355"; flake = false; };
    contrib-google-protobuf = { url = "github:ClickHouse/google-protobuf/0fae801fb4785175a4481aae1c0f721700e7bd99"; flake = false; };
    contrib-boost = { url = "github:ClickHouse/boost/ae94606a70f1e298ce2a5718db858079185c4d9c"; flake = false; };
    contrib-arrow = { url = "github:ClickHouse/arrow/210f686ab96733180bed04e36098911825d70345"; flake = false; };
    contrib-thrift = { url = "github:apache/thrift/2a93df80f27739ccabb5b885cb12a8dc7595ecdf"; flake = false; };
    contrib-libhdfs3 = { url = "github:ClickHouse/libhdfs3/d0ae7d2568151feef61d3ec7896803262f0e0f91"; flake = false; };
    contrib-libxml2 = { url = "github:GNOME/libxml2/8d509f483dd5ce268b2fded9c738132c47d820d8"; flake = false; };
    contrib-libgsasl = { url = "github:ClickHouse/libgsasl/2d16b4e0d9435bec4546875ef07d36383bb993a5"; flake = false; };
    contrib-snappy = { url = "github:ClickHouse/snappy/6ebb5b1ab8801ea3fde103c5c29f5ab86df5fe7a"; flake = false; };
    contrib-cppkafka = { url = "github:ClickHouse/cppkafka/114d5be53330390a16c7ee227ef5e3551a9b5f49"; flake = false; };
    contrib-brotli = { url = "github:google/brotli/63be8a99401992075c23e99f7c84de1c653e39e2"; flake = false; };
    contrib-h3 = { url = "github:ClickHouse/h3/c7f46cfd71fb60e2fefc90e28abe81657deff735"; flake = false; };
    contrib-simdjson = { url = "github:simdjson/simdjson/e341c8b43861b43de29c48ab65f292d997096953"; flake = false; };
    contrib-rapidjson = { url = "github:ClickHouse/rapidjson/800ca2f38fc3b387271d9e1926fcfc9070222104"; flake = false; };
    contrib-fastops = { url = "github:ClickHouse/fastops/1460583af7d13c0e980ce46aec8ee9400314669a"; flake = false; };
    contrib-orc = { url = "github:ClickHouse/orc/1892a6539e655280d38a8d0669214dbb9b1d0919"; flake = false; };
    contrib-sparsehash-c11 = { url = "github:sparsehash/sparsehash-c11/cf0bffaa456f23bc4174462a789b90f8b6f5f42f"; flake = false; };
    contrib-grpc = { url = "github:ClickHouse/grpc/fb3ee40ec8383f0c83b110d3d422f4a25c6f28ce"; flake = false; };
    contrib-aws = { url = "github:ClickHouse/aws-sdk-cpp/a86b913abc2795ee23941b24dd51e862214ec6b0"; flake = false; };
    contrib-aws-c-event-stream = { url = "github:awslabs/aws-c-event-stream/08f24e384e5be20bcffa42b49213d24dad7881ae"; flake = false; };
    contrib-aws-c-common = { url = "github:awslabs/aws-c-common/80f21b3cac5ac51c6b8a62c7d2a5ef58a75195ee"; flake = false; };
    contrib-aws-checksums = { url = "github:awslabs/aws-checksums/321b805559c8e911be5bddba13fcbd222a3e2d3a"; flake = false; };
    contrib-curl = { url = "github:curl/curl/4dacb79fcdd9364c1083e06f6a011d797a344f47"; flake = false; };
    contrib-icudata = { url = "github:ClickHouse/icudata/cfc05b4c3140ff2be84291b80de8c62b1e42d0da"; flake = false; };
    contrib-icu = { url = "github:ClickHouse/icu/4216173eeeb39c1d4caaa54a68860e800412d273"; flake = false; };
    contrib-flatbuffers = { url = "github:ClickHouse/flatbuffers/0100f6a5779831fa7a651e4b67ef389a8752bd9b"; flake = false; };
    contrib-replxx = { url = "github:ClickHouse/replxx/dea4b7c724a2ec41d078383ccf0ebeedfaebaeea"; flake = false; };
    contrib-avro = { url = "github:ClickHouse/avro/545e7002683cbc2198164d93088ac8e4955b4628"; flake = false; };
    contrib-msgpack-c = { url = "github:msgpack/msgpack-c/46684265d50b5d1b062d4c5c428ba08462844b1d"; flake = false; };
    contrib-libcpuid = { url = "github:anrieff/libcpuid/018a05372d1cf2b4b9c18192da6f203521a23460"; flake = false; };
    contrib-openldap = { url = "github:ClickHouse/openldap/5671b80e369df2caf5f34e02924316205a43c895"; flake = false; };
    contrib-AMQP-CPP = { url = "github:ClickHouse/AMQP-CPP/00f09897ce020a84e38f87dc416af4a19c5da9ae"; flake = false; };
    contrib-cassandra = { url = "github:ClickHouse/cpp-driver/f4a31e92a25c34c02c7291ff97c7813bc83b0e09"; flake = false; };
    contrib-libuv = { url = "github:ClickHouse/libuv/714b58b9849568211ade86b44dd91d37f8a2175e"; flake = false; };
    contrib-fmtlib = { url = "github:fmtlib/fmt/123913715afeb8a437e6388b4473fcc4753e1c9a"; flake = false; };
    contrib-krb5 = { url = "github:ClickHouse/krb5/ed01212a0ba6e101f51178b9014368af062c01c4"; flake = false; };
    contrib-cyrus-sasl = { url = "github:ClickHouse/cyrus-sasl/e6466edfd638cc5073debe941c53345b18a09512"; flake = false; };
    contrib-croaring = { url = "github:RoaringBitmap/CRoaring/9b7cc0ff1c41e9457efb6228cfd2c538d0155303"; flake = false; };
    contrib-miniselect = { url = "github:danlark1/miniselect/be0af6bd0b6eb044d1acc4f754b229972d99903a"; flake = false; };
    contrib-rocksdb = { url = "github:ClickHouse/rocksdb/4d479be3591e7855cecebfb894585e04cd9e4245"; flake = false; };
    contrib-xz = { url = "github:xz-mirror/xz/869b9d1b4edd6df07f819d360d306251f8147353"; flake = false; };
    contrib-abseil-cpp = { url = "github:ClickHouse/abseil-cpp/9ac7062b1860d895fb5a8cbf58c3e9ef8f674b5f"; flake = false; };
    contrib-dragonbox = { url = "github:ClickHouse/dragonbox/923705af6fd953aa948fc175f6020b15f7359838"; flake = false; };
    contrib-fast_float = { url = "github:fastfloat/fast_float/7eae925b51fd0f570ccd5c880c12e3e27a23b86f"; flake = false; };
    contrib-NuRaft = { url = "github:ClickHouse/NuRaft/c01f5cd9e0edb90c4febf49685b962cf0de91833"; flake = false; };
    contrib-datasketches-cpp = { url = "github:apache/datasketches-cpp/76edd74f5db286b672c170a8ded4ce39b3a8800f"; flake = false; };
    contrib-yaml-cpp = { url = "github:ClickHouse/yaml-cpp/f91e938341273b5f9d341380ab17bcc3de5daa06"; flake = false; };
    contrib-cld2 = { url = "github:ClickHouse/cld2/217ba8b8805b41557faadaa47bb6e99f2242eea3"; flake = false; };
    contrib-libstemmer_c = { url = "github:ClickHouse/libstemmer_c/c753054304d87daf460057c1a649c482aa094835"; flake = false; };
    contrib-wordnet-blast = { url = "github:ClickHouse/wordnet-blast/1d16ac28036e19fe8da7ba72c16a307fbdf8c87e"; flake = false; };
    contrib-lemmagen-c = { url = "github:ClickHouse/lemmagen-c/59537bdcf57bbed17913292cb4502d15657231f1"; flake = false; };
    contrib-libpqxx = { url = "github:ClickHouse/libpqxx/24a31c3f3a9317131b1326c7b87f42053a5f4489"; flake = false; };
    contrib-sqlite-amalgamation = { url = "github:ClickHouse/sqlite-amalgamation/20598079891d27ef1a3ad3f66bbfa3f983c25268"; flake = false; };
    contrib-s2geometry = { url = "github:ClickHouse/s2geometry/6522a40338d58752c2a4227a3fc2bc4107c73e43"; flake = false; };
    contrib-bzip2 = { url = "github:ClickHouse/bzip2/bf905ea2251191ff9911ae7ec0cfc35d41f9f7f6"; flake = false; };
    contrib-magic_enum = { url = "github:Neargye/magic_enum/1a1824df7ac798177a521eed952720681b0bf482"; flake = false; };
    contrib-libprotobuf-mutator = { url = "github:google/libprotobuf-mutator/b922c8ab9004ef9944982e4f165e2747b13223fa"; flake = false; };
    contrib-sysroot = { url = "github:ClickHouse/sysroot/265c3be3412eda7abd00bbf55095f0af2f3f57a0"; flake = false; };
    contrib-nlp-data = { url = "github:ClickHouse/nlp-data/5591f91f5e748cba8fb9ef81564176feae774853"; flake = false; };
    contrib-hive-metastore = { url = "github:ClickHouse/hive-metastore/809a77d435ce218d9b000733f19489c606fc567b"; flake = false; };
    contrib-azure = { url = "github:ClickHouse/azure-sdk-for-cpp/67272b7ee0adff6b69921b26eb071ba1a353062c"; flake = false; };
    contrib-minizip-ng = { url = "github:zlib-ng/minizip-ng/95ba7abdd24a956bde584db54d1d55e37d511e2f"; flake = false; };
    contrib-qpl = { url = "github:intel/qpl/c2ced94c53c1ee22191201a59878e9280bc9b9b8"; flake = false; };
    contrib-idxd-config = { url = "github:ClickHouse/idxd-config/99a72fbfec55746e43254116baa22efb4cba85cc"; flake = false; };
    contrib-QAT-ZSTD-Plugin = { url = "github:intel/QAT-ZSTD-Plugin/e5a134e12d2ea8a5b0f3b83c5b1c325fda4eb0a8"; flake = false; };
    contrib-qatlib = { url = "github:intel/qatlib/abe15d7bfc083117bfbb4baee0b49ffcd1c03c5c"; flake = false; };
    contrib-wyhash = { url = "github:wangyi-fudan/wyhash/991aa3dab624e50b066f7a02ccc9f6935cc740ec"; flake = false; };
    contrib-nats-io = { url = "github:ClickHouse/nats.c/cf441828d30fdd5de12d9da319e88d2586fdeeba"; flake = false; };
    contrib-vectorscan = { url = "github:VectorCamp/vectorscan/d29730e1cb9daaa66bda63426cdce83505d2c809"; flake = false; };
    contrib-llvm-project = { url = "github:ClickHouse/llvm-project/7e72cd6d7ae4cfabf1cc4a77eb2787ece492bb8b"; flake = false; };
    contrib-corrosion = { url = "github:corrosion-rs/corrosion/c4840742d23d1c1a187152e2c5ae65886b9c9007"; flake = false; };
    contrib-libssh = { url = "github:ClickHouse/libssh/ed4011b91873836713576475a98cd132cd834539"; flake = false; };
    contrib-morton-nd = { url = "github:morton-nd/morton-nd/3795491a4aa3cdc916c8583094683f0d68df5bc0"; flake = false; };
    contrib-xxHash = { url = "github:Cyan4973/xxHash/bbb27a5efb85b92a0486cf361a8635715a53f6ba"; flake = false; };
    contrib-crc32-s390x = { url = "github:linux-on-ibm-z/crc32-s390x/30980583bf9ed3fa193abb83a1849705ff457f70"; flake = false; };
    contrib-google-benchmark = { url = "github:google/benchmark/2257fa4d6afb8e5a2ccd510a70f38fe7fcdf1edf"; flake = false; };
    contrib-libdivide = { url = "github:ridiculousfish/libdivide/01526031eb79375dc85e0212c966d2c514a01234"; flake = false; };
    contrib-libbcrypt = { url = "github:rg3/libbcrypt/8aa32ad94ebe06b76853b0767c910c9fbf7ccef4"; flake = false; };
    contrib-ulid-c = { url = "github:ClickHouse/ulid-c/c433b6783cf918b8f996dacd014cb2b68c7de419"; flake = false; };
    contrib-aws-crt-cpp = { url = "github:ClickHouse/aws-crt-cpp/e5aa45cacfdcda7719ead38760e7c61076f5745f"; flake = false; };
    contrib-aws-c-io = { url = "github:ClickHouse/aws-c-io/11ce3c750a1dac7b04069fc5bff89e97e91bad4d"; flake = false; };
    contrib-aws-c-mqtt = { url = "github:awslabs/aws-c-mqtt/6d36cd3726233cb757468d0ea26f6cd8dad151ec"; flake = false; };
    contrib-aws-c-auth = { url = "github:awslabs/aws-c-auth/baeffa791d9d1cf61460662a6d9ac2186aaf05df"; flake = false; };
    contrib-aws-c-cal = { url = "github:ClickHouse/aws-c-cal/1586846816e6d7d5ff744a2db943107a3a74a082"; flake = false; };
    contrib-aws-c-sdkutils = { url = "github:awslabs/aws-c-sdkutils/fd8c0ba2e233997eaaefe82fb818b8b444b956d3"; flake = false; };
    contrib-aws-c-http = { url = "github:awslabs/aws-c-http/a082f8a2067e4a31db73f1d4ffd702a8dc0f7089"; flake = false; };
    contrib-aws-c-s3 = { url = "github:awslabs/aws-c-s3/de36fee8fe7ab02f10987877ae94a805bf440c1f"; flake = false; };
    contrib-aws-c-compression = { url = "github:awslabs/aws-c-compression/99ec79ee2970f1a045d4ced1501b97ee521f2f85"; flake = false; };
    contrib-crc32-vpmsum = { url = "github:antonblanchard/crc32-vpmsum/452155439389311fc7d143621eaf56a258e02476"; flake = false; };
    contrib-liburing = { url = "github:axboe/liburing/08468cc3830185c75f9e7edefd88aa01e5c2f8ab"; flake = false; };
    contrib-libarchive = { url = "github:libarchive/libarchive/8a7a9cc527fd1d6d8664315d3bed47c4259479cc"; flake = false; };
    contrib-libfiu = { url = "github:ClickHouse/libfiu/a1290d8cd3d7b4541d6c976e0a54f572ac03f2a3"; flake = false; };
    contrib-isa-l = { url = "github:ClickHouse/isa-l/9f2b68f05752097f0f16632fc4a9a86950831efd"; flake = false; };
    contrib-c-ares = { url = "github:c-ares/c-ares/d3a507e920e7af18a5efb7f9f1d8044ed4750013"; flake = false; };
    contrib-incbin = { url = "github:graphitemaster/incbin/6e576cae5ab5810f25e2631f2e0b80cbe7dc8cbf"; flake = false; };
    contrib-usearch = { url = "github:ClickHouse/usearch/07e14f83337b3ad067d2557c88da3501f961b5c6"; flake = false; };
    contrib-SimSIMD = { url = "github:ClickHouse/SimSIMD/bee6bb499ab526007fc5d5289f1d998aa1eb6249"; flake = false; };
    contrib-FP16 = { url = "github:Maratyszcza/FP16/0a92994d729ff76a58f692d3028ca1b64b145d91"; flake = false; };
    contrib-aklomp-base64 = { url = "github:aklomp/base64/e77bd70bdd860c52c561568cffb251d88bba064c"; flake = false; };
    contrib-pocketfft = { url = "github:mreineck/pocketfft/f4c1aa8aa9ce79ad39e80f2c9c41b92ead90fda3"; flake = false; };
    contrib-sqids-cpp = { url = "github:sqids/sqids-cpp/a471f53672e98d49223f598528a533b07b085c61"; flake = false; };
    contrib-idna = { url = "github:ada-url/idna/3c8be01d42b75649f1ac9b697d0ef757eebfe667"; flake = false; };
    contrib-google-cloud-cpp = { url = "github:ClickHouse/google-cloud-cpp/83f30caadb8613fb5c408d8c2fd545291596b53f"; flake = false; };
    contrib-rust_vendor = { url = "github:ClickHouse/rust_vendor/f2d7604c8857ea1faa68d7220c67687a2ae84720"; flake = false; };
    contrib-openssl = { url = "github:ClickHouse/openssl/2aa34c68d677b447fb85c55167d8d1ab98ba4def"; flake = false; };
    contrib-double-conversion = { url = "github:ClickHouse/double-conversion/4f7a25d8ced8c7cf6eee6fd09d6788eaa23c9afe"; flake = false; };
    contrib-mongo-cxx-driver = { url = "github:ClickHouse/mongo-cxx-driver/3166bdb49b717ce1bc30f46cc2b274ab1de7005b"; flake = false; };
    contrib-mongo-c-driver = { url = "github:ClickHouse/mongo-c-driver/aabf2f86682aab0a7329160929b8f6654a18216a"; flake = false; };
    contrib-numactl = { url = "github:ClickHouse/numactl/ff32c618d63ca7ac48cce366c5a04bb3563683a0"; flake = false; };
    contrib-postgres = { url = "github:ClickHouse/postgres/9da0420d861df7542b92058342afd4723e95ef4c"; flake = false; };
    contrib-delta-kernel-rs = { url = "github:ClickHouse/delta-kernel-rs/beb2eb808e8c7ae6992c99809331b7cc2ed85278"; flake = false; };
    contrib-SHA3IUF = { url = "github:brainhub/SHA3IUF/fc8504750a5c2174a1874094dd05e6a0d8797753"; flake = false; };
    contrib-chdig = { url = "github:azat/chdig/e5d3cfdb163c6fdb2c9f1359eda8d22d0db38a0e"; flake = false; };
  };
}
