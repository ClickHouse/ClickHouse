const WideTableWrapper = ({ children }) => {
  const containerStyle = {
    overflow: "auto",
    maxWidth: "100%",
  };
  // Mintlify はインラインスタイルをサポートしており、MDX の children も忠実にレンダリングするため —
  // コンテンツの可読性を確保するには、このシンプルなラッパーで十分です。Docusaurus 元実装の
  // 上部スクロールバーを同期する UX はあると便利ですが、
  // コンテンツを損なうことなく省略できます。
  return <div style={containerStyle}>{children}</div>;
};
export default WideTableWrapper;