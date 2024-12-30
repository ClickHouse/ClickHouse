# Laion-400M データセット

[Laion-400M データセット](https://laion.ai/blog/laion-400-open-dataset/) は、400百万枚の画像と英語の画像キャプションを含んでいます。Laion は現在、[さらに大きなデータセット](https://laion.ai/blog/laion-5b/) を提供していますが、その操作は同様です。

このデータセットには、画像のURL、画像とそのキャプションの埋め込み、画像とキャプション間の類似度スコア、およびメタデータ（例えば、画像の幅/高さ、ライセンス、NSFWフラグ）が含まれています。ClickHouse で[近似最近傍探索](../../engines/table-engines/mergetree-family/annindexes.md)を実演するためにこのデータセットを使用することができます。

## データの準備

埋め込みとメタデータは生データの別々のファイルに保存されています。データ準備のステップでは、データをダウンロードしてファイルをマージし、それらを CSV に変換して ClickHouse にインポートします。以下の `download.sh` スクリプトを使用できます:

```bash
number=${1}
if [[ $number == '' ]]; then
    number=1
fi;
wget --tries=100 https://deploy.laion.ai/8f83b608504d46bb81708ec86e912220/embeddings/img_emb/img_emb_${number}.npy          # 画像埋め込みをダウンロード
wget --tries=100 https://deploy.laion.ai/8f83b608504d46bb81708ec86e912220/embeddings/text_emb/text_emb_${number}.npy        # テキスト埋め込みをダウンロード
wget --tries=100 https://deploy.laion.ai/8f83b608504d46bb81708ec86e912220/embeddings/metadata/metadata_${number}.parquet    # メタデータをダウンロード
python3 process.py $number # ファイルをマージし、CSV に変換
```

`process.py` は以下のように定義されます:

```python
import pandas as pd
import numpy as np
import os
import sys

str_i = str(sys.argv[1])
npy_file = "img_emb_" + str_i + '.npy'
metadata_file = "metadata_" + str_i + '.parquet'
text_npy =  "text_emb_" + str_i + '.npy'

# すべてのファイルを読み込む
im_emb = np.load(npy_file)
text_emb = np.load(text_npy) 
data = pd.read_parquet(metadata_file)

# ファイルを結合
data = pd.concat([data, pd.DataFrame({"image_embedding" : [*im_emb]}), pd.DataFrame({"text_embedding" : [*text_emb]})], axis=1, copy=False)

# ClickHouse にインポートするカラム
data = data[['url', 'caption', 'NSFW', 'similarity', "image_embedding", "text_embedding"]]

# np.array をリストに変換
data['image_embedding'] = data['image_embedding'].apply(lambda x: list(x))
data['text_embedding'] = data['text_embedding'].apply(lambda x: list(x))

# キャプションに様々な引用符が含まれるための小さなハック
data['caption'] = data['caption'].apply(lambda x: x.replace("'", " ").replace('"', " "))

# データを CSV ファイルとしてエクスポート
data.to_csv(str_i + '.csv', header=False)

# 生データファイルを削除
os.system(f"rm {npy_file} {metadata_file} {text_npy}")
```

データ準備パイプラインを開始するには、以下を実行します:

```bash
seq 0 409 | xargs -P1 -I{} bash -c './download.sh {}'
```

データセットは410個のファイルに分割されており、各ファイルには約100万行が含まれています。データの小さなサブセットで作業したい場合は、例えば `seq 0 9 | ...` として制限を調整してください。

(上記のPythonスクリプトは非常に遅く（ファイルあたり約2-10分）、大量のメモリ（ファイルあたり41 GB）を消費し、生成されるcsvファイルも大きい（各10GB）ため注意が必要です。十分なRAMがある場合は、より多くの並列処理を実行するために `-P1` の数を増やしてください。それでも遅すぎる場合は、より良いインジェスション手順、例えば .npy ファイルを parquet に変換してから ClickHouse で処理する方法を検討してください。)

## テーブルの作成

インデックスなしのテーブルを作成するには、次を実行します:

```sql
CREATE TABLE laion
(
    `id` Int64,
    `url` String,
    `caption` String,
    `NSFW` String,
    `similarity` Float32,
    `image_embedding` Array(Float32),
    `text_embedding` Array(Float32)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 8192
```

CSVファイルをClickHouseにインポートするには：

```sql
INSERT INTO laion FROM INFILE '{path_to_csv_files}/*.csv'
```

## ブルートフォースのANN検索を実行する（ANNインデックスなし）

ブルートフォースの近似最近傍探索を実行するには、次を実行します：

```sql
SELECT url, caption FROM laion ORDER BY L2Distance(image_embedding, {target:Array(Float32)}) LIMIT 30
```

`target` は 512 要素の配列で、クライアントパラメータです。このような配列を取得する便利な方法は記事の最後で紹介されます。今のところ、ランダムな猫の画像の埋め込みを `target` として実行できます。

**結果**

```
┌─url───────────────────────────────────────────────────────────────────────────────────────────────────────────┬─caption────────────────────────────────────────────────────────────────┐
│ https://s3.amazonaws.com/filestore.rescuegroups.org/6685/pictures/animals/13884/13884995/63318230_463x463.jpg │ Adoptable Female Domestic Short Hair                                   │
│ https://s3.amazonaws.com/pet-uploads.adoptapet.com/8/b/6/239905226.jpg                                        │ Adopt A Pet :: Marzipan - New York, NY                                 │
│ http://d1n3ar4lqtlydb.cloudfront.net/9/2/4/248407625.jpg                                                      │ Adopt A Pet :: Butterscotch - New Castle, DE                           │
│ https://s3.amazonaws.com/pet-uploads.adoptapet.com/e/e/c/245615237.jpg                                        │ Adopt A Pet :: Tiggy - Chicago, IL                                     │
│ http://pawsofcoronado.org/wp-content/uploads/2012/12/rsz_pumpkin.jpg                                          │ Pumpkin an orange tabby  kitten for adoption                           │
│ https://s3.amazonaws.com/pet-uploads.adoptapet.com/7/8/3/188700997.jpg                                        │ Adopt A Pet :: Brian the Brad Pitt of cats - Frankfort, IL             │
│ https://s3.amazonaws.com/pet-uploads.adoptapet.com/8/b/d/191533561.jpg                                        │ Domestic Shorthair Cat for adoption in Mesa, Arizona - Charlie         │
│ https://s3.amazonaws.com/pet-uploads.adoptapet.com/0/1/2/221698235.jpg                                        │ Domestic Shorthair Cat for adoption in Marietta, Ohio - Daisy (Spayed) │
└───────────────────────────────────────────────────────────────────────────────────────────────────────────────┴────────────────────────────────────────────────────────────────────────┘

8 rows in set. Elapsed: 6.432 sec. Processed 19.65 million rows, 43.96 GB (3.06 million rows/s., 6.84 GB/s.)
```

## ANNインデックスでANNを実行する

ANNインデックスを持つ新しいテーブルを作成し、既存のテーブルからデータを挿入します：

```sql
CREATE TABLE laion_annoy
(
    `id` Int64,
    `url` String,
    `caption` String,
    `NSFW` String,
    `similarity` Float32,
    `image_embedding` Array(Float32),
    `text_embedding` Array(Float32),
    INDEX annoy_image image_embedding TYPE annoy(),
    INDEX annoy_text text_embedding TYPE annoy()
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 8192;

INSERT INTO laion_annoy SELECT * FROM laion;
```

デフォルトでは、Annoy インデックスは L2 距離をメトリックとして使用します。インデックス作成と検索のための詳細な調整は、Annoy インデックスの[ドキュメント](../../engines/table-engines/mergetree-family/annindexes.md)で説明されています。同じクエリで再度確認してみましょう：

```sql
SELECT url, caption FROM laion_annoy ORDER BY l2Distance(image_embedding, {target:Array(Float32)}) LIMIT 8
```

**結果**

```
┌─url──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┬─caption──────────────────────────────────────────────────────────────┐
│ http://tse1.mm.bing.net/th?id=OIP.R1CUoYp_4hbeFSHBaaB5-gHaFj                                                                                                                         │ bed bugs and pets can cats carry bed bugs pets adviser               │
│ http://pet-uploads.adoptapet.com/1/9/c/1963194.jpg?336w                                                                                                                              │ Domestic Longhair Cat for adoption in Quincy, Massachusetts - Ashley │
│ https://thumbs.dreamstime.com/t/cat-bed-12591021.jpg                                                                                                                                 │ Cat on bed Stock Image                                               │
│ https://us.123rf.com/450wm/penta/penta1105/penta110500004/9658511-portrait-of-british-short-hair-kitten-lieing-at-sofa-on-sun.jpg                                                    │ Portrait of british short hair kitten lieing at sofa on sun.         │
│ https://www.easypetmd.com/sites/default/files/Wirehaired%20Vizsla%20(2).jpg                                                                                                          │ Vizsla (Wirehaired) image 3                                          │
│ https://images.ctfassets.net/yixw23k2v6vo/0000000200009b8800000000/7950f4e1c1db335ef91bb2bc34428de9/dog-cat-flickr-Impatience_1.jpg?w=600&h=400&fm=jpg&fit=thumb&q=65&fl=progressive │ dog and cat image                                                    │
│ https://i1.wallbox.ru/wallpapers/small/201523/eaa582ee76a31fd.jpg                                                                                                                    │ cats, kittens, faces, tonkinese                                      │
│ https://www.baxterboo.com/images/breeds/medium/cairn-terrier.jpg                                                                                                                     │ Cairn Terrier Photo                                                  │
└──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┴──────────────────────────────────────────────────────────────────────┘

8 rows in set. Elapsed: 0.641 sec. Processed 22.06 thousand rows, 49.36 MB (91.53 thousand rows/s., 204.81 MB/s.)
```

速度は大幅に向上しましたが、より正確ではない結果になっています。これは、ANN インデックスが近似的な検索結果のみを提供するためです。例では類似した画像埋め込みを検索しましたが、画像キャプションの埋め込みを検索することも可能です。

## UDFを使用した埋め込みの作成

通常、新しい画像や画像キャプションの埋め込みを作成し、データ中の類似画像/画像キャプションのペアを検索したくなります。クライアントを離れることなく `target` ベクトルを作成するために[UDF](../../sql-reference/functions/index.md#sql-user-defined-functions) を使用できます。データの作成と検索用に新しい埋め込みを作成する際には同じモデルを使用することが重要です。以下のスクリプトは、データセットの基礎となる `ViT-B/32` モデルを利用しています。

### テキスト埋め込み

まず、次のPythonスクリプトを ClickHouse のデータパスにある `user_scripts/` ディレクトリに保存し、実行可能にします (`chmod +x encode_text.py`)。

`encode_text.py`:

```python
#!/usr/bin/python3
import clip
import torch
import numpy as np
import sys

if __name__ == '__main__':
    device = "cuda" if torch.cuda.is_available() else "cpu"
    model, preprocess = clip.load("ViT-B/32", device=device)
    for text in sys.stdin:
        inputs = clip.tokenize(text)
        with torch.no_grad():
            text_features = model.encode_text(inputs)[0].tolist()
            print(text_features)
        sys.stdout.flush()
```

次に、ClickHouse サーバーの設定ファイルで `<user_defined_executable_functions_config>/path/to/*_function.xml</user_defined_executable_functions_config>` に参照されている場所に `encode_text_function.xml` を作成します。

```xml
<functions>
    <function>
        <type>executable</type>
        <name>encode_text</name>
        <return_type>Array(Float32)</return_type>
        <argument>
            <type>String</type>
            <name>text</name>
        </argument>
        <format>TabSeparated</format>
        <command>encode_text.py</command>
        <command_read_timeout>1000000</command_read_timeout>
    </function>
</functions>
```

今すぐ次のように使えます：

```sql
SELECT encode_text('cat');
```

初回の実行はモデルが読み込まれるため遅くなりますが、繰り返すと速くなります。出力結果をコピーして `SET param_target=...` に簡単に書き込むことができます。

### 画像埋め込み

画像埋め込みは、ローカル画像へのパスを画像キャプションテキストの代わりにPythonスクリプトに提供することで同様に作成できます。

`encode_image.py`

```python
#!/usr/bin/python3
import clip
import torch
import numpy as np
from PIL import Image
import sys

if __name__ == '__main__':
    device = "cuda" if torch.cuda.is_available() else "cpu"
    model, preprocess = clip.load("ViT-B/32", device=device)
    for text in sys.stdin:
        image = preprocess(Image.open(text.strip())).unsqueeze(0).to(device)
        with torch.no_grad():
            image_features = model.encode_image(image)[0].tolist()
            print(image_features)
        sys.stdout.flush()
```

`encode_image_function.xml`

```xml
<functions>
    <function>
        <type>executable_pool</type>
        <name>encode_image</name>
        <return_type>Array(Float32)</return_type>
        <argument>
            <type>String</type>
            <name>path</name>
        </argument>
        <format>TabSeparated</format>
        <command>encode_image.py</command>
        <command_read_timeout>1000000</command_read_timeout>
    </function>
</functions>
```

次にこのクエリを実行します：

```sql
SELECT encode_image('/path/to/your/image');
```

