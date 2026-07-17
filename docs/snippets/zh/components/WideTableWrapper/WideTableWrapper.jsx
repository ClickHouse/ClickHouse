const WideTableWrapper = ({ children }) => {
  const containerStyle = {
    overflow: "auto",
    maxWidth: "100%",
  };
  // Mintlify 支持内联样式，并且能准确渲染 MDX 子内容 —
  // 这个简单的包装组件就足以保证内容可读性。Docusaurus 原版中
  // 顶部滚动条的同步交互体验算是锦上添花，我们可以
  // 省略它而不影响内容。
  return <div style={containerStyle}>{children}</div>;
};
export default WideTableWrapper;