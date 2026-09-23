const WideTableWrapper = ({ children }) => {
  const containerStyle = {
    overflow: "auto",
    maxWidth: "100%",
  };
  // Mintlify는 인라인 스타일을 지원하고 MDX children도 충실하게 렌더링합니다 —
  // 따라서 콘텐츠 가독성에는 이 정도의 단순한 래퍼만으로도 충분합니다. 원래
  // Docusaurus의 상단 스크롤바 UX는 있으면 좋지만, 콘텐츠에 문제를 일으키지
  // 않는다면 생략해도 됩니다.
  return <div style={containerStyle}>{children}</div>;
};
export default WideTableWrapper;