i/**
 * lunr对中文分词的支持
 */
;
(function(root, factory) {
  if (typeof define === 'function' && define.amd) {
    // AMD. Register as an anonymous module.
    define(factory)
  } else if (typeof exports === 'object') {
    /**
     * Node. Does not work with strict CommonJS, but
     * only CommonJS-like environments that support module.exports,
     * like Node.
     */
    module.exports = factory()
  } else {
    // Browser globals (root is window)
    factory()(root.lunr);
  }
}(this, function() {
  /**
   * Just return a value to define the module export.
   * This example returns an object, but the module
   * can return a function as the exported value.
   */
  return function(lunr) { 
    
    /*
    Thai tokenization is the same to Japanense, which does not take into account spaces.
    So, it uses the same logic to assign tokenization function due to different Lunr versions.
    */
    var isLunr2 = lunr.version[0] == "2";

    /* register specific locale function */
    lunr.zhcn = function() {
        this.pipeline.reset();
        this.pipeline.add(
            lunr.zhcn.trimmer,
            lunr.zhcn.stopWordFilter,
            lunr.zhcn.stemmer
        );

        if (isLunr2) { // for lunr version 2.0.0
            this.tokenizer = lunr.zhcn.tokenizer;
        } else {
            if (lunr.tokenizer) { // for lunr version 0.6.0
                lunr.tokenizer = lunr.zhcn.tokenizer;
            }
            if (this.tokenizerFn) { // for lunr version 0.7.0 -> 1.0.0
                this.tokenizerFn = lunr.zhcn.tokenizer;
            }
        }
    };


    var segmenter = new lunr.TinySegmenter(); 

    lunr.zhcn.tokenizer = function(obj) {
      var i;
      var str;
      var len;
      var segs;
      var tokens;
      var char;
      var sliceLength;
      var sliceStart;
      var sliceEnd;
      var segStart;

      if (!arguments.length || obj == null || obj == undefined)
        return [];

      if (Array.isArray(obj)) {
        return obj.map(
          function(t) {
            return isLunr2 ? new lunr.Token(t.toLowerCase()) : t.toLowerCase();
          }
        );
      }

      str = obj.toString().toLowerCase().replace(/^\s+/, '');
      for (i = str.length - 1; i >= 0; i--) {
        if (/\S/.test(str.charAt(i))) {
          str = str.substring(0, i + 1);
          break;
        }
      }

      tokens = [];
      len = str.length;
      for (sliceEnd = 0, sliceStart = 0; sliceEnd <= len; sliceEnd++) {
        char = str.charAt(sliceEnd);
        sliceLength = sliceEnd - sliceStart;

        if ((char.match(/\s/) || sliceEnd == len)) {
          if (sliceLength > 0) {
            segs = segmenter.segment(str.slice(sliceStart, sliceEnd)).filter(
              function(token) {
                return !!token;
              }
            );

            segStart = sliceStart;
            for (i = 0; i < segs.length; i++) {
              if (isLunr2) {
                tokens.push(
                  new lunr.Token(
                    segs[i], {
                      position: [segStart, segs[i].length],
                      index: tokens.length
                    }
                  )
                );
              } else {
                tokens.push(segs[i]);
              }
              segStart += segs[i].length;
            }
          }

          sliceStart = sliceEnd + 1;
        }
      }

      return tokens;
    }

    lunr.zhcn.stemmer = (function(){
      return function(word) {
        return word;
      }
    })();
    
    lunr.Pipeline.registerFunction(lunr.zhcn.stemmer, 'stemmer-zhcn');

    /* lunr trimmer function */
    lunr.zhcn.wordCharacters = "一二三四五六七八九十百千万億兆一-龠々〆ヵヶぁ-んァ-ヴーｱ-ﾝﾞa-zA-Zａ-ｚＡ-Ｚ0-9０-９";
    lunr.zhcn.trimmer = lunr.trimmerSupport.generateTrimmer(lunr.zhcn.wordCharacters);
    lunr.Pipeline.registerFunction(lunr.zhcn.trimmer, 'trimmer-zhcn');


    /* lunr stop word filter. see https://www.ranks.nl/stopwords/chinese-stopwords */
    lunr.zhcn.stopWordFilter = lunr.generateStopWordFilter('的 一 不 在 人 有 是 为 以 于 上 他 而 后 之 来 及 了 因 下 可 到 由 这 与 也 此 但 并 个 其 已 无 小 我 们 起 最 再 今 去 好 只 又 或 很 亦 某 把 那 你 乃 它 吧 被 比 别 趁 当 从 到 得 打 凡 儿 尔 该 各 给 跟 和 何 还 即 几 既 看 据 距 靠 啦 了 另 么 每 们 嘛 拿 哪 那 您 凭 且 却 让 仍 啥 如 若 使 谁 虽 随 同 所 她 哇 嗡 往 哪 些 向 沿 哟 用 于 咱 则 怎 曾 至 致 着 诸 自'.split(' '));
    lunr.Pipeline.registerFunction(lunr.zhcn.stopWordFilter, 'stopWordFilter-zhcn');
  };
}))
